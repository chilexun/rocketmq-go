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

type producerClient interface {
	getPublishTopics() []string
}

type consumerClient interface {
	getSubscribeTopics() []string
}

//MQClient cache, key:clientID; value:*MQClient
var clientMap sync.Map

//A MQClient represent the a rocketmq client object related to Producer and Consumer
type MQClient struct {
	clientID        string
	config          *ClientConfig
	rpcClient       RPCClient
	status          ServiceState
	executer        *TimeWheel
	hbState         int32
	brokerVerMap    sync.Map
	nameServAddrs   []string
	nameserv        atomic.Value
	nameservLock    sync.Mutex
	producerClients sync.Map //key:producerGroup; value:producerClient
	consumerClients sync.Map //key:consumerGroup; value:consumerClient
	topicLock       sync.Mutex
	topicManager    TopicRouteInfoManager
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
	exists := false
	f := func(k, v interface{}) bool {
		exists = true
		return false
	}
	client.producerClients.Range(f)
	client.consumerClients.Range(f)
	if exists {
		return
	}
	if atomic.CompareAndSwapInt32(&client.status, Running, ShutdownAlready) {
		client.executer.Stop()
		client.rpcClient.CloseAllConns()
	}
}

//RegisterProducer to MQ client, return false is producer group already registed
func (client *MQClient) RegisterProducer(producerGroup string, producer producerClient) bool {
	_, loaded := client.producerClients.LoadOrStore(producerGroup, producer)
	return !loaded
}

//UnregisterProducer from MQ client
func (client *MQClient) UnregisterProducer(producerGroup string) {
	client.producerClients.Delete(producerGroup)
}

//RegisterConsumer to MQ Client, return false if consumer group already registed
func (client *MQClient) RegisterConsumer(consumerGroup string, consumer consumerClient) bool {
	_, loaded := client.consumerClients.LoadOrStore(consumerGroup, consumer)
	return !loaded
}

//UnregisterConsumer from MQ client
func (client *MQClient) UnregisterConsumer(consumerGroup string) {
	client.consumerClients.Delete(consumerGroup)
}

//SendMessageRequest accept a msg request and return a send result response
func (client *MQClient) SendMessageRequest(brokerAddr string, mq *MessageQueue,
	request *SendMessageRequest, timeout time.Duration) (*SendMessageResponse, error) {
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

func (client *MQClient) PullMessageRequest(brokerAddr string, request *PullMessageRequest,
	timeout time.Duration) (*PullResult, error) {
	return nil, nil
}

func (client *MQClient) PullMessageAsyncRequest(brokerAddr string, request *PullMessageRequest,
	callback PullCallback, timeout time.Duration) error {
	return nil
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
		for name, addr := range client.topicManager.GetBrokerAddrTable() {
			client.sendHeartbeatToBroker(name, addr)
		}
	}
}

func (client *MQClient) sendHeartbeatToBroker(brokerName string, brokerAddr string) {
	heartbeat := HeartbeatData{ClientID: client.clientID}
	groups := client.GetAllProducerGroups()
	if len(groups) <= 0 {
		return
	}
	heartbeat.ProducerDataSet = make([]ProducerData, len(groups))
	for i, group := range groups {
		heartbeat.ProducerDataSet[i] = ProducerData{group}
	}
	cmd := HeartBeat(&heartbeat)
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
		verMap := make(map[string]int)
		verMap[brokerAddr] = version
		client.brokerVerMap.Store(brokerName, verMap)
	}
	if rand.Intn(20) == 1 {
		logger.Infof("Heartbeat to broker %s, version: %d", brokerAddr, version)
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

	usedTopics := make(map[string]bool)
	client.producerClients.Range(func(k, v interface{}) bool {
		p := v.(producerClient)
		for _, topic := range p.getPublishTopics() {
			usedTopics[topic] = true
		}
		return true
	})
	client.consumerClients.Range(func(k, v interface{}) bool {
		c := v.(consumerClient)
		for _, topic := range c.getSubscribeTopics() {
			usedTopics[topic] = true
		}
		return true
	})
	for topic := range usedTopics {
		client.syncTopicFromNameserv(topic)
	}
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

//GetTopicPublishInfo returns the mqs of the specific topic
func (client *MQClient) GetTopicSubscribeInfo(topic string) []MessageQueue {
	return client.topicManager.GetTopicSubscribeInfo(topic)
}

//GetBrokerAddrByName returns the master broker addr. Will try to sync topic route if no addr was found
func (client *MQClient) GetBrokerAddrByName(topic string, brokerName string, vipPrefer bool) string {
	addr := client.topicManager.GetBrokerAddrByName(brokerName, vipPrefer)
	if addr == "" {
		client.syncTopicFromNameservWithLock(topic)
		addr = client.topicManager.GetBrokerAddrByName(brokerName, vipPrefer)
	}
	return addr
}

//GetBrokerAddrByTopic returns master broker addr
//If the master's address cannot be found, a slave broker address is selected in a random manner.
func (client *MQClient) GetBrokerAddrByTopic(topic string) string {
	routeData := client.topicManager.GetTopicRouteData(topic)
	if routeData != nil && routeData.brokerDatas != nil {
		brokers := routeData.brokerDatas
		index := rand.Intn(len(brokers))
		brokerData := brokers[index]
		masterAddr := brokerData.brokerAddrs[MasterBrokerID]
		if masterAddr == "" {
			index = rand.Intn(len(brokerData.brokerAddrs))
			return brokerData.brokerAddrs[index]
		}
	}
	return ""
}

func (client *MQClient) syncTopicFromNameservWithLock(topic string) {
	client.topicLock.Lock()
	defer client.topicLock.Unlock()
	client.syncTopicFromNameserv(topic)
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
		routeData, err := parseTopicRouteData(body)
		if err != nil {
			logger.Error("Decode topic route data failed, topic:"+topic, err)
			return
		}
		client.topicManager.SetTopicRouteData(topic, routeData)
	}
}

func parseTopicRouteData(body []byte) (*TopicRouteData, error) {
	kvMap, err := ParseObject(body)
	if err != nil {
		return nil, err
	}
	routeData := &TopicRouteData{
		queueDatas:        make([]QueueData, 0),
		filterServerTable: make(map[string][]string),
	}
	for k, v := range kvMap {
		switch k {
		case "orderTopicConf":
			routeData.OrderTopicConf = string(v)
		case "queueDatas":
			err = json.Unmarshal(v, &routeData.queueDatas)
			if err != nil {
				return nil, err
			}
		case "brokerDatas":
			bd, err := ParseArray(v)
			if err != nil {
				return nil, err
			}
			brokers := make([]BrokerData, len(bd))
			for i, d := range bd {
				broker, err := parseBrokerData(d)
				if err != nil {
					return nil, err
				}
				brokers[i] = *broker
			}
			routeData.brokerDatas = brokers
		case "filterServerTable":
			err = json.Unmarshal(v, &routeData.filterServerTable)
			if err != nil {
				return nil, err
			}
		}
	}
	return routeData, nil
}

func parseBrokerData(d []byte) (*BrokerData, error) {
	m, err := ParseObject(d)
	if err != nil {
		return nil, err
	}
	broker := &BrokerData{
		cluster:     string(m["cluster"]),
		brokerName:  string(m["brokerName"]),
		brokerAddrs: make(map[int]string),
	}

	d, ok := m["brokerAddrs"]
	if !ok {
		return broker, nil
	}

	m, err = ParseObject(d)
	if err != nil {
		return nil, err
	}

	for k, v := range m {
		id, err := strconv.Atoi(string(k))
		if err != nil {
			return nil, err
		}
		broker.brokerAddrs[int(id)] = string(v)
	}

	return broker, nil
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
		client.nameserv.Store(addrList[index])
		if _, err := client.rpcClient.GetActiveConn(addrList[index]); err == nil {
			return addrList[index]
		}
		index++
		index %= len(addrList)
	}
	return ""
}

func (client *MQClient) GetAllPublishTopics() []string {
	topics := make(map[string]bool)
	result := make([]string, 0)
	client.producerClients.Range(func(k, v interface{}) bool {
		p := v.(producerClient)
		for _, t := range p.getPublishTopics() {
			if !topics[t] {
				result = append(result, t)
			}
			topics[t] = true
		}
		return true
	})
	return result
}

func (client *MQClient) GetAllSubscribeTopics() []string {
	topics := make(map[string]bool)
	result := make([]string, 0)
	client.consumerClients.Range(func(k, v interface{}) bool {
		c := v.(consumerClient)
		for _, t := range c.getSubscribeTopics() {
			if !topics[t] {
				result = append(result, t)
			}
			topics[t] = true
		}
		return true
	})
	return result
}

func (client *MQClient) GetAllProducerGroups() []string {
	groups := make([]string, 0)
	client.producerClients.Range(func(k, v interface{}) bool {
		groups = append(groups, k.(string))
		return true
	})
	return groups
}

func (client *MQClient) GetAllConsumerGroups() []string {
	groups := make([]string, 0)
	client.consumerClients.Range(func(k, v interface{}) bool {
		groups = append(groups, k.(string))
		return true
	})
	return groups
}

func (client *MQClient) FindConsumerIDs(topic string, consumerGroup string) []string {
	brokerAddr := client.GetBrokerAddrByTopic(topic)
	if brokerAddr == "" {
		client.syncTopicFromNameservWithLock(topic)
		brokerAddr = client.GetBrokerAddrByTopic(topic)
	}
	if brokerAddr == "" {
		return nil
	}

	req := GetConsumersByGroup(consumerGroup)
	resp, err := client.rpcClient.InvokeSync(BrokerVIPAddr(brokerAddr), &req, 3*time.Second)
	if err != nil {
		logger.Errorf("RPC error:", err)
	}
	if ResponseCode(resp.Code) != Success {
		logger.Errorf("Get consumer IDs error, broker:%s resp:[code=%d, remark=%s]", brokerAddr, resp.Code, resp.Remark)
	}

	cidsData := new(ConsumerListByGroupResponse)
	err = json.Unmarshal(resp.Body, cidsData)
	if err != nil {
		logger.Errorf("Invalid response body of GetConsumerIDsByGroup, broker:%s", brokerAddr)
	}
	return cidsData.ConsumerIdList
}
