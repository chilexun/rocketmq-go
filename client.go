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
	CREATE_JUST ServiceState = iota
	RUNNING
	SHUTDOWN_ALREADY
	START_FAILED
)

var clientMap sync.Map

type MQClient struct {
	clientID        string
	config          *ClientConfig
	rpcClient       RPCClient
	status          ServiceState
	executer        *TimeWheel
	hbState         int32
	brokerMap       sync.Map
	brokerVerMap    sync.Map
	nameServAddrs   []string
	preNameservAddr string
	nameservLock    sync.Mutex
	refCount        int32
	topicLock       sync.Mutex
	topicManager    TopicRouteInfoManager
	usedTopics      sync.Map
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
		status:    CREATE_JUST,
		executer:  NewTimeWheel(time.Second, 120),
	}
	value, ok = clientMap.LoadOrStore(clientID, instance)
	if ok {
		return value.(*MQClient)
	}

	return instance
}

func (this *MQClient) GetAllUsingTopics() {

}

func (this *MQClient) Start() (err error) {
	if !atomic.CompareAndSwapInt32(&this.status, CREATE_JUST, START_FAILED) {
		if this.status != RUNNING {
			err = errors.New("MQClient " + this.clientID + " cannot be started, current status is " + strconv.Itoa(int(this.status)))
		}
		return
	}
	if this.config.NamesrvAddr == nil || len(this.config.NamesrvAddr) == 0 {
		this.fetchNameServAddr()
	} else {
		this.nameServAddrs = this.config.NamesrvAddr
	}
	this.startScheduledTask()
	atomic.StoreInt32(&this.status, RUNNING)
	return nil
}

func (this *MQClient) Shutdown() {
	if len(GetAllProducerGroups()) > 0 {
		return
	}
	if atomic.CompareAndSwapInt32(&this.status, RUNNING, SHUTDOWN_ALREADY) {
		this.executer.Stop()
		this.rpcClient.CloseAllConns()
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

func (this *MQClient) startScheduledTask() {
	//fetch nameserv addrs if no nameserv supplied
	this.executer.AddJob(10*time.Second, 2*time.Minute, this.fetchNameServAddr)
	//sync topic router from nameserv
	this.executer.AddJob(10*time.Millisecond, this.config.PollNameServerInterval, this.SyncTopicFromNameserv)
	//send heartbeat to active brokers
	this.executer.AddJob(1*time.Second, this.config.HeartbeatBrokerInterval, this.SendHeartbeatToBrokers)
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

func (this *MQClient) fetchNameServAddr() {
	if len(this.config.WsAddr) == 0 {
		return
	}
	client := &http.Client{
		Timeout: 3 * time.Second,
	}
	resp, err := client.Get(this.config.WsAddr)
	if err != nil || resp.StatusCode != 200 {
		//log err
		return
	}
	defer resp.Body.Close()
	body, rerr := ioutil.ReadAll(resp.Body)
	if rerr != nil {
		//log err
		return
	}

	this.nameservLock.Lock()
	defer this.nameservLock.Unlock()
	respStr := strings.Trim(string(body), "\r\n ")
	if len(body) > 0 {
		this.nameServAddrs = strings.Split(respStr, ";")
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
		//log no nameserv error
		return
	}
	resp, err := client.rpcClient.InvokeSync(addr, &req, 3*time.Second)
	if err != nil {
		//log error
		return
	}
	code, body := resp.Code, resp.Body
	if ResponseCode(code) == TOPIC_NOT_EXIST {
		//log error
	} else if ResponseCode(code) == SUCCESS {
		routeData := new(TopicRouteData)
		err := json.Unmarshal(body, &routeData)
		if err != nil {
			//log error
			return
		}
		client.topicManager.SetTopicRouteData(topic, routeData)
	}
}

func (client *MQClient) selectActiveNameServ() string {
	if _, err := client.rpcClient.GetActiveConn(client.preNameservAddr); err == nil {
		return client.preNameservAddr
	}
	client.nameservLock.Lock()
	defer client.nameservLock.Unlock()
	if _, err := client.rpcClient.GetActiveConn(client.preNameservAddr); err == nil {
		return client.preNameservAddr
	}
	addrList := client.nameServAddrs
	index := rand.Intn(len(addrList))
	for i := 0; i < len(addrList); i++ {
		if client.preNameservAddr != addrList[index] {
			client.preNameservAddr = addrList[index]
			if _, err := client.rpcClient.GetActiveConn(client.preNameservAddr); err == nil {
				return client.preNameservAddr
			}
		}
		index++
		index %= len(addrList)
	}
	return ""
}
