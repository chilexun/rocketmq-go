package mqclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//Client constants
const (
	ClientInnerProducerGroup  string = "CLIENT_INNER_PRODUCER_GROUP"
	DefaultClientInstanceName string = "DEFAULT"
)

//ServiceState is a alias type of int32 and represent the running state of client
type ServiceState = int32

const (
	//CreateJust is the initial state of client
	CreateJust ServiceState = iota
	//Running if client started success
	Running
	//ShutdownAlready after client is shutdown, it is a terminal state
	ShutdownAlready
	//StartFailed if try to start client failed, it is a terminal state
	StartFailed
)

var clientMap sync.Map

//A MQClient represent the a rocketmq client object related to Producer and Consumer
type MQClient struct {
	clientID      string
	config        *ClientConfig
	rpcClient     RPCClient
	status        ServiceState
	executer      *TimeWheel
	hbState       int32
	brokerVerMap  sync.Map
	nameServAddrs []string
	nameserv      atomic.Value
	nameservLock  sync.Mutex
	refCount      int32
	topicLock     sync.Mutex
	topicManager  TopicRouteInfoManager
	usedTopics    sync.Map
}

//GetClientInstance create a new client if instanceName had not been used
func GetClientInstance(config *ClientConfig, instanceName string) *MQClient {
	clientID := fmt.Sprint(GetIPAddr()) + "@" + instanceName
	value, ok := clientMap.Load(clientID)
	if ok {
		return value.(*MQClient)
	}

	instance := &MQClient{
		clientID:  clientID,
		config:    config,
		rpcClient: NewRPCClient(config),
		status:    CreateJust,
		executer:  NewTimeWheel(time.Second, 120),
	}
	value, ok = clientMap.LoadOrStore(clientID, instance)
	if ok {
		return value.(*MQClient)
	}

	return instance
}

//Start client jobs and set status to Running
func (client *MQClient) Start() (err error) {
	if !atomic.CompareAndSwapInt32(&client.status, CreateJust, StartFailed) {
		if client.status != Running {
			err = errors.New("MQClient " + client.clientID + " cannot be started, current status is " + strconv.Itoa(int(client.status)))
		}
		return
	}
	if client.config.NamesrvAddr == nil || len(client.config.NamesrvAddr) == 0 {
		client.fetchNameServAddr()
	} else {
		client.nameServAddrs = client.config.NamesrvAddr
	}
	client.startScheduledTask()
	atomic.StoreInt32(&client.status, Running)
	return
}

//Shutdown will stop the jobs and close connections if reference count gt 0
func (client *MQClient) Shutdown() {
	if atomic.LoadInt32(&client.refCount) > 0 {
		return
	}
	if atomic.CompareAndSwapInt32(&client.status, Running, ShutdownAlready) {
		client.executer.Stop()
		client.rpcClient.CloseAllConns()
	}
}

//IncrReference increase client reference count by 1
func (client *MQClient) IncrReference() int32 {
	return atomic.AddInt32(&client.refCount, 1)
}

//DecrReference decrease client reference count by 1
func (client *MQClient) DecrReference() int32 {
	return atomic.AddInt32(&client.refCount, -1)
}

//SendMessageRequest accept a msg request and return a send result response
func (client *MQClient) SendMessageRequest(brokerAddr string, mq *MessageQueue,
	request *SendMessageRequest, timeout time.Duration) (*SendMessageResponse, error) {
	client.usedTopics.Store(mq.Topic, true)
	cmd := SendMessage(request)
	respCmd, err := client.rpcClient.InvokeSync(brokerAddr, &cmd, timeout)
	if err != nil {
		return nil, err
	}

	response := new(SendMessageResponse)
	err = response.fromExtFields(respCmd.ExtFields)
	response.Code = ResponseCode(respCmd.Code)
	response.Remark = respCmd.Remark
	return response, err
}

func (client *MQClient) startScheduledTask() {
	client.executer.Start()
	//fetch nameserv addrs if no nameserv supplied
	client.executer.AddJob(10*time.Second, 2*time.Minute, client.fetchNameServAddr)
	//sync topic router from nameserv
	client.executer.AddJob(10*time.Millisecond, client.config.PollNameServerInterval, client.SyncTopicFromNameserv)
	//send heartbeat to active brokers
	client.executer.AddJob(1*time.Second, client.config.HeartbeatBrokerInterval, client.SendHeartbeatToBrokers)
}

//SendHeartbeatToBrokers send heartbeat to all of master brokers
func (client *MQClient) SendHeartbeatToBrokers() {
	if atomic.CompareAndSwapInt32(&client.hbState, 0, 1) {
		defer atomic.StoreInt32(&client.hbState, 0)
		client.topicManager.brokerAddrTable.Range(func(k interface{}, v interface{}) bool {
			brokers := v.(map[int]string)
			for brokerID, brokerAddr := range brokers {
				if brokerID == 1 {
					client.sendHeartbeatToBroker(k.(string), brokerAddr)
				}
			}
			return true
		})
	}
}

func (client *MQClient) sendHeartbeatToBroker(brokerName string, brokerAddr string) {
	heartbeat := HeartbeatData{clientID: client.clientID}
	groups := GetAllProducerGroups()
	if len(groups) <= 0 {
		return
	}
	heartbeat.producerDataSet = make([]ProducerData, len(groups))
	for i, group := range groups {
		heartbeat.producerDataSet[i] = ProducerData{group}
	}
	cmd := HeartBeat(heartbeat)
	resp, err := client.rpcClient.InvokeSync(brokerAddr, &cmd, 3*time.Second)
	if err != nil {
		logger.Errorf("Send heartbeat to broker %s(%s), Error:%s", brokerName, brokerAddr, err)
		return
	} else if ResponseCode(resp.Code) != Success {
		logger.Errorf("Receive invalid heartbeat response from broker %s(%s), Error Code:%d", brokerName, brokerAddr, resp.Code)
		return
	}
	version := resp.Version
	if verMap, ok := client.brokerVerMap.Load(brokerName); ok {
		verMap.(map[string]int)[brokerAddr] = version
	} else {
		newMap := make(map[string]int, 4)
		newMap[brokerAddr] = version
		client.brokerVerMap.Store(brokerName, verMap)
	}
}

func (client *MQClient) fetchNameServAddr() {
	if len(client.config.WsAddr) == 0 {
		return
	}
	httpClient := &http.Client{
		Timeout: 3 * time.Second,
	}
	resp, err := httpClient.Get(client.config.WsAddr)
	if err != nil || resp.StatusCode != 200 {
		logger.Error("Fail to fetch nameserv addr from :", client.config.WsAddr)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("Read name serve addrs error, ", err)
		return
	}

	client.nameservLock.Lock()
	defer client.nameservLock.Unlock()
	respStr := strings.Trim(string(body), "\r\n ")
	if len(body) > 0 {
		client.nameServAddrs = strings.Split(respStr, ";")
	}
}

//SyncTopicFromNameserv fetch topic routes from nameserv and cache
func (client *MQClient) SyncTopicFromNameserv() {
	client.topicLock.Lock()
	defer client.topicLock.Unlock()

	client.usedTopics.Range(func(k, value interface{}) bool {
		client.syncTopicFromNameserv(k.(string))
		return true
	})
}

//GetTopicPublishInfo load topic route from cache  and try to fetch from nameserv if no cache
func (client *MQClient) GetTopicPublishInfo(topic string) *TopicPublishInfo {
	publishInfo := client.topicManager.GetTopicPublishInfo(topic)
	if publishInfo != nil {
		return publishInfo
	}

	client.topicLock.Lock()
	defer client.topicLock.Unlock()
	publishInfo = client.topicManager.GetTopicPublishInfo(topic)
	if publishInfo != nil {
		return publishInfo
	}
	client.syncTopicFromNameserv(topic)
	return client.topicManager.GetTopicPublishInfo(topic)
}

//GetBrokerAddrByName returns the master broker addr. Will try to sync topic route if no addr was found
func (client *MQClient) GetBrokerAddrByName(topic string, brokerName string, vipPrefer bool) string {
	addr := client.topicManager.GetBrokerAddrByName(brokerName, vipPrefer)
	if addr == "" {
		client.topicLock.Lock()
		defer client.topicLock.Unlock()
		client.syncTopicFromNameserv(topic)
		addr = client.topicManager.GetBrokerAddrByName(brokerName, vipPrefer)
	}
	return addr
}

func (client *MQClient) syncTopicFromNameserv(topic string) {
	req := GetRouteInfo(topic)
	addr := client.selectActiveNameServ()
	if len(addr) == 0 {
		logger.Error("No active name serve")
		return
	}
	resp, err := client.rpcClient.InvokeSync(addr, &req, 3*time.Second)
	if err != nil {
		logger.Error("Error occurs when try to get route info", err)
		return
	}
	code, body := ResponseCode(resp.Code), resp.Body
	if code == TopicNotExist {
		logger.Errorf("Get route failed since TOPIC NOT EXIST, topic:%s", topic)
	} else if code == Success {
		routeData := new(TopicRouteData)
		err := json.Unmarshal(body, &routeData)
		if err != nil {
			logger.Error("Decode topic route data failed, topic:"+topic, err)
			return
		}
		client.topicManager.SetTopicRouteData(topic, routeData)
	}
}

func (client *MQClient) selectActiveNameServ() string {
	preValue := client.nameserv.Load()
	if preValue != nil && client.rpcClient.IsConnActive(preValue.(string)) {
		return preValue.(string)
	}

	client.nameservLock.Lock()
	defer client.nameservLock.Unlock()
	preValue = client.nameserv.Load()
	if preValue != nil && client.rpcClient.IsConnActive(preValue.(string)) {
		return preValue.(string)
	}
	addrList := client.nameServAddrs
	index := rand.Intn(len(addrList))
	for i := 0; i < len(addrList); i++ {
		if preValue == nil || preValue.(string) != addrList[index] {
			client.nameserv.Store(addrList[index])
			if _, err := client.rpcClient.GetActiveConn(addrList[index]); err == nil {
				return addrList[index]
			}
		}
		index++
		index %= len(addrList)
	}
	return ""
}
