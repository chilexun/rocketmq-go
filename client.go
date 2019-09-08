package mqclient

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ServiceState = int32

const (
	CreateJust ServiceState = iota
	Running
	ShutdownAlready
	StartFailed
)

var clientMap sync.Map

type MQClient struct {
	clientID      string
	config        *ClientConfig
	rpcClient     RPCClient
	status        ServiceState
	executer      *TimeWheel
	hbState       int32
	brokerMap     sync.Map
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
	clientID := GetIPAddr() + "@" + instanceName
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

func (client *MQClient) Shutdown() {
	if atomic.LoadInt32(&client.refCount) > 0 {
		return
	}
	if atomic.CompareAndSwapInt32(&client.status, Running, ShutdownAlready) {
		client.executer.Stop()
		client.rpcClient.CloseAllConns()
	}
}

func (client *MQClient) IncrReference() int32 {
	return atomic.AddInt32(&client.refCount, 1)
}

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
	respValue := ResolveStruct(respCmd.ExtFields, reflect.TypeOf((*SendMessageResponse)(nil)).Elem())
	response := respValue.Interface().(SendMessageResponse)
	response.Code = ResponseCode(respCmd.Code)
	response.Remark = respCmd.Remark
	return &response, nil
}

func (client *MQClient) startScheduledTask() {
	//fetch nameserv addrs if no nameserv supplied
	client.executer.AddJob(10*time.Second, 2*time.Minute, client.fetchNameServAddr)
	//sync topic router from nameserv
	client.executer.AddJob(10*time.Millisecond, client.config.PollNameServerInterval, client.SyncTopicFromNameserv)
	//send heartbeat to active brokers
	client.executer.AddJob(1*time.Second, client.config.HeartbeatBrokerInterval, client.SendHeartbeatToBrokers)
}

func (this *MQClient) SendHeartbeatToBrokers() {
	if atomic.CompareAndSwapInt32(&this.hbState, 0, 1) {
		defer atomic.StoreInt32(&this.hbState, 0)
		this.brokerMap.Range(func(k interface{}, v interface{}) bool {
			brokers := v.(map[int]string)
			for brokerID, brokerAddr := range brokers {
				if brokerID == 1 {
					this.sendHeartbeatToBroker(brokerAddr)
				}
			}
			return true
		})
	}
}

func (this *MQClient) sendHeartbeatToBroker(brokerAddr string) {
	heartbeat := HeartbeatData{clientID: this.clientID}
	groups := GetAllProducerGroups()
	if len(groups) <= 0 {
		return
	}
	heartbeat.producerDataSet = make([]ProducerData, len(groups))
	for i, group := range groups {
		heartbeat.producerDataSet[i] = ProducerData{group}
	}
	cmd := HeartBeat(heartbeat)
	resp, err := this.rpcClient.InvokeSync(brokerAddr, &cmd, 3*time.Second)
	if err != nil {
		//log error
		return
	} else if ResponseCode(resp.Code) != SUCCESS {
		//log code and err msg
		return
	}
	version := resp.Version
	if verMap, ok := this.brokerVerMap.Load(brokerAddr); ok {
		verMap.(map[string]int)[brokerAddr] = version
	} else {
		newMap := make(map[string]int, 4)
		newMap[brokerAddr] = version
		this.brokerVerMap.Store(brokerAddr, verMap)
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

func (client *MQClient) SyncTopicFromNameserv() {
	client.topicLock.Lock()
	defer client.topicLock.Unlock()

	client.usedTopics.Range(func(k, value interface{}) bool {
		client.syncTopicFromNameserv(k.(string))
		return true
	})
}

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
	if code == TOPIC_NOT_EXIST {
		logger.Errorf("Get route failed since TOPIC NOT EXIST, topic:%s", topic)
	} else if code == SUCCESS {
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
