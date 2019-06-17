package mqclient

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
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

var once sync.Once
var instance *MQClient

type MQClient struct {
	clientId        string
	config          *Config
	rpcClient       RpcClient
	fastClient      RpcClient
	status          ServiceState
	executer        *TimeWheel
	hbState         int32
	brokerMap       sync.Map
	brokerVerMap    sync.Map
	nameServAddrs   []string
	preNameservAddr string
	nameservLock    sync.Mutex
}

func GetClientInstance(config *Config, instanceName string) *MQClient {
	once.Do(func() {
		clientId := GetIPAddr() + "@" + instanceName
		instance = new(MQClient)
		instance.clientId = clientId
		instance.config = config
		rpcClient := NewRpcClient(config)
		instance.rpcClient = rpcClient
		fastClient := NewRpcClient(config)
		instance.fastClient = fastClient
		instance.status = CREATE_JUST
		instance.executer = NewTimeWheel(time.Second, 120)
	})
	return instance
}

func (this *MQClient) GetAllUsingTopics() {

}

func (this *MQClient) Start() (err error) {
	if !atomic.CompareAndSwapInt32((*int32)(&this.status), int32(CREATE_JUST), int32(START_FAILED)) {
		if this.status != RUNNING {
			err = errors.New("MQClient " + this.clientId + " cannot be started, current status is " + strconv.Itoa(int(this.status)))
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
		CloseAllConns()
	}
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
	heartbeat := HeartbeatData{clientID: this.clientId}
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
	} else if ResponseCode(resp.Code) != SUCCESS {
		//log code and err msg
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
	client := &http.Client{
		Timeout: 3 * time.Second,
	}
	resp, err := client.Get(this.config.WsAddr)
	if err != nil || resp.StatusCode != 200 {
		//log err
	}
	body, rerr := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if rerr != nil {
		//log err
	}
	//todo : check thread safe
	respStr := strings.Trim(string(body), "\r\n ")
	if len(body) > 0 {
		this.nameServAddrs = strings.Split(respStr, ";")
	}
}

func (this *MQClient) SyncTopicFromNameserv() {
	topics := GetAllPublishTopics()
	for _, topic := range topics {
		req := GetRouteInfo(topic)
		addr := this.selectActiveNameServ()
		if len(addr) == 0 {
			//log no nameserv error
		}
		resp, err := this.rpcClient.InvokeSync(addr, &req, 3*time.Second)
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
			SetTopicRouteData(topic, routeData)
		}
	}
}

func (this *MQClient) selectActiveNameServ() string {
	if this.rpcClient.GetOrCreateActiveConn(this.preNameservAddr) {
		return this.preNameservAddr
	}
	this.nameservLock.Lock()
	defer this.nameservLock.Unlock()
	if this.rpcClient.GetOrCreateActiveConn(this.preNameservAddr) {
		return this.preNameservAddr
	}
	addrList := this.nameServAddrs
	index := rand.Intn(len(addrList))
	for i := 0; i < len(addrList); i++ {
		if this.preNameservAddr != addrList[index] {
			this.preNameservAddr = addrList[index]
			if this.rpcClient.GetOrCreateActiveConn(this.preNameservAddr) {
				return this.preNameservAddr
			}
		}
		index++
		index /= len(addrList)
	}
	return ""
}
