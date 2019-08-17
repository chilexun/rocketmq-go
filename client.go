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

type ServiceState int32

const (
	CREATE_JUST ServiceState = iota
	RUNNING
	SHUTDOWN_ALREADY
	START_FAILED
)

var clientMap sync.Map

type MQClient struct {
	clientID        string
	config          *Config
	rpcClient       RPCClient
	fastRPCClient   RPCClient
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
}

//GetClientInstance create a new client if instanceName had not been used
func GetClientInstance(config *Config, instanceName string) *MQClient {
	clientID := GetIPAddr() + "@" + instanceName
	value, ok := clientMap.Load(clientID)
	if ok {
		return value.(*MQClient)
	}

	instance := &MQClient{
		clientID:      clientID,
		config:        config,
		rpcClient:     NewRPCClient(config),
		fastRPCClient: NewRPCClient(config),
		status:        CREATE_JUST,
		executer:      NewTimeWheel(time.Second, 120),
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
	if !atomic.CompareAndSwapInt32((*int32)(&this.status), int32(CREATE_JUST), int32(START_FAILED)) {
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
	this.status = RUNNING
	return nil
}

func (this *MQClient) Shutdown() {
	if len(GetAllProducerGroups()) > 0 {
		return
	}
	if atomic.CompareAndSwapInt32((*int32)(&this.status), int32(RUNNING), int32(SHUTDOWN_ALREADY)) {
		this.executer.Stop()
		this.rpcClient.CloseAllConns()
		this.fastRPCClient.CloseAllConns()
	}
}

func (client *MQClient) IncrReference() int32 {
	return atomic.AddInt32(&client.refCount, 1)
}

func (client *MQClient) DecrReference() int32 {
	return atomic.AddInt32(&client.refCount, -1)
}

func (this *MQClient) SyncRequest(addr string, cmd *Command, t reflect.Type, timeout time.Duration) (*Command, error) {
	return this.rpcClient.InvokeSync(addr, cmd, t, timeout)
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
			for brokerId, brokerAddr := range brokers {
				if brokerId == 1 {
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
	resp, err := this.rpcClient.InvokeSync(brokerAddr, &cmd, nil, 3*time.Second)
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
	body, rerr := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if rerr != nil {
		//log err
		return
	}
	//todo : check thread safe
	respStr := strings.Trim(string(body), "\r\n ")
	if len(body) > 0 {
		this.nameServAddrs = strings.Split(respStr, ";")
	}
}

func (client *MQClient) SyncTopicFromNameserv() {
	client.topicLock.Lock()
	defer client.topicLock.Unlock()

	topics := GetAllPublishTopics()
	for _, topic := range topics {
		client.syncTopicFromNameserv(topic)
	}
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
	}
	resp, err := client.rpcClient.InvokeSync(addr, &req, nil, 3*time.Second)
	if err != nil {
		//log error
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

func (this *MQClient) selectActiveNameServ() string {
	if _, err := this.rpcClient.GetActiveConn(this.preNameservAddr); err == nil {
		return this.preNameservAddr
	}
	this.nameservLock.Lock()
	defer this.nameservLock.Unlock()
	if _, err := this.rpcClient.GetActiveConn(this.preNameservAddr); err == nil {
		return this.preNameservAddr
	}
	addrList := this.nameServAddrs
	index := rand.Intn(len(addrList))
	for i := 0; i < len(addrList); i++ {
		if this.preNameservAddr != addrList[index] {
			this.preNameservAddr = addrList[index]
			if _, err := this.rpcClient.GetActiveConn(this.preNameservAddr); err == nil {
				return this.preNameservAddr
			}
		}
		index++
		index /= len(addrList)
	}
	return ""
}
