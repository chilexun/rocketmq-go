package mqclient

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

//MasterBrokerID is the ID of master in cluster
const (
	MasterBrokerID int = 0
)

//MessageQueue represents a queue of specefic topic on the broker
type MessageQueue struct {
	Topic      string
	BrokerName string
	QueueID    int
}

//QueueData represents the broker queue info
type QueueData struct {
	BrokerName     string
	ReadQueueNums  int
	WriteQueueNums int
	Perm           Permission
	TopicSynFlag   int
}

//BrokerData represents the broker cluster info
type BrokerData struct {
	cluster     string
	brokerName  string
	brokerAddrs map[int]string
}

//TopicRouteData represents the route data
type TopicRouteData struct {
	OrderTopicConf    string
	queueDatas        []QueueData
	brokerDatas       []BrokerData
	filterServerTable map[string][]string
}

//TopicPublishInfo represent the publish route
type TopicPublishInfo struct {
	OrderTopic     bool
	HasRouteInfo   bool
	MsgQueues      []MessageQueue
	topicRoute     *TopicRouteData
	nextQueueIndex int32
}

//TopicRouteInfoManager represent and cache the cluster route info
type TopicRouteInfoManager struct {
	topicRouteTable  sync.Map
	brokerAddrTable  sync.Map
	publishInfoTable sync.Map
	subscribeTable   sync.Map
}

//SetTopicRouteData update topic route info in the cache
func (m *TopicRouteInfoManager) SetTopicRouteData(topic string, data *TopicRouteData) {
	for _, brokerData := range data.brokerDatas {
		m.brokerAddrTable.Store(brokerData.brokerName, brokerData.brokerAddrs)
	}
	m.publishInfoTable.Store(topic, toPublishInfo(topic, data))
	m.subscribeTable.Store(topic, toSubscribeInfo(topic, data))
	m.topicRouteTable.Store(topic, cloneRouteData(data))
}

func toPublishInfo(topic string, route *TopicRouteData) *TopicPublishInfo {
	info := new(TopicPublishInfo)
	info.HasRouteInfo = true
	info.topicRoute = route
	if len(route.OrderTopicConf) > 0 {
		brokers := strings.Split(route.OrderTopicConf, ";")
		for _, broker := range brokers {
			item := strings.Split(broker, ":")
			nums, err := strconv.Atoi(item[1])
			if err != nil {
				for i := 0; i < nums; i++ {
					mq := MessageQueue{topic, item[0], i}
					info.MsgQueues = append(info.MsgQueues, mq)
				}
			}
		}
		info.OrderTopic = true
	} else {
		qds := route.queueDatas
		sort.SliceStable(qds, func(i, j int) bool {
			return qds[i].BrokerName < qds[j].BrokerName
		})
		for _, qd := range qds {
			if PermitWrite(qd.Perm) {
				for _, bd := range route.brokerDatas {
					if bd.brokerName == qd.BrokerName {
						if _, ok := bd.brokerAddrs[MasterBrokerID]; ok {
							for i := 0; i < qd.WriteQueueNums; i++ {
								mq := MessageQueue{topic, qd.BrokerName, i}
								info.MsgQueues = append(info.MsgQueues, mq)
							}
						}
						break
					}
				}
			}
		}
		info.OrderTopic = false
	}

	return info
}

func toSubscribeInfo(topic string, route *TopicRouteData) []MessageQueue {
	queues := make([]MessageQueue, 0)
	for _, qd := range route.queueDatas {
		if PermitRead(Permission(qd.Perm)) {
			for i := 0; i < qd.ReadQueueNums; i++ {
				mq := MessageQueue{topic, qd.BrokerName, i}
				queues = append(queues, mq)
			}
		}
	}
	return queues
}

func cloneRouteData(data *TopicRouteData) *TopicRouteData {
	cloneRouteData := new(TopicRouteData)
	cloneRouteData.OrderTopicConf = data.OrderTopicConf
	cloneRouteData.brokerDatas = make([]BrokerData, 0)
	cloneRouteData.brokerDatas = append(cloneRouteData.brokerDatas, data.brokerDatas...)
	cloneRouteData.queueDatas = make([]QueueData, 0)
	cloneRouteData.queueDatas = append(cloneRouteData.queueDatas, data.queueDatas...)
	return cloneRouteData
}

//GetTopicPublishInfo returns the publish info of the topic
func (m *TopicRouteInfoManager) GetTopicPublishInfo(topic string) *TopicPublishInfo {
	if v, ok := m.publishInfoTable.Load(topic); ok {
		return v.(*TopicPublishInfo)
	}
	return nil
}

//GetQueueNumber returns the number of queues in the topic
func (t *TopicPublishInfo) GetQueueNumber() int {
	return len(t.MsgQueues)
}

//GetWriteQueueNumber returns the num of write queues
func (t *TopicPublishInfo) GetWriteQueueNumber(brokerName string) int {
	for _, queueData := range t.topicRoute.queueDatas {
		if queueData.BrokerName == brokerName {
			return queueData.WriteQueueNums
		}
	}
	return -1
}

//GetNextQueue select a next queue id
func (t *TopicPublishInfo) GetNextQueue() MessageQueue {
	nextIndex := atomic.AddInt32(&t.nextQueueIndex, 1)
	pos := int(nextIndex) % len(t.MsgQueues)
	if pos < 0 {
		pos = 0
	}
	return t.MsgQueues[pos]
}

//GetTopicSubscribeInfo returns consumer topic subscription info
func (m *TopicRouteInfoManager) GetTopicSubscribeInfo() {

}

//GetBrokerAddrByName return broker address by broker name
func (m *TopicRouteInfoManager) GetBrokerAddrByName(brokerName string, vipPrefer bool) string {
	clusterTable, ok := m.brokerAddrTable.Load(brokerName)
	if ok {
		addr, ok := clusterTable.(map[int]string)[MasterBrokerID]
		if ok {
			if vipPrefer {
				ipAndPort := strings.Split(addr, ":")
				port, _ := strconv.Atoi(ipAndPort[1])
				return ipAndPort[0] + ":" + strconv.Itoa(port-2)
			}
			return addr
		}
	}
	return ""
}

//GetBrokerAddrTable returns a copy of cached brokers, key: brokerName, value: brokerAddress
func (m *TopicRouteInfoManager) GetBrokerAddrTable() map[string]string {
	result := make(map[string]string)
	m.brokerAddrTable.Range(func(k interface{}, v interface{}) bool {
		brokers := v.(map[int]string)
		for brokerID, brokerAddr := range brokers {
			if brokerID == MasterBrokerID {
				result[k.(string)] = brokerAddr
			}
		}
		return true
	})
	return result
}
