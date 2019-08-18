package mqclient

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type QueueData struct {
	BrokerName     string
	ReadQueueNums  int
	WriteQueueNums int
	Perm           Permission
	TopicSynFlag   int
}

type BrokerData struct {
	cluster     string
	brokerName  string
	brokerAddrs map[int]string
}

type TopicRouteData struct {
	OrderTopicConf    string
	queueDatas        []QueueData
	brokerDatas       []BrokerData
	filterServerTable map[string][]string
}

type TopicPublishInfo struct {
	OrderTopic     bool
	HasRouteInfo   bool
	MsgQueues      []MessageQueue
	topicRoute     *TopicRouteData
	nextQueueIndex int32
}

type TopicRouteInfoManager struct {
	topicRouteTable  sync.Map
	brokerAddrTable  sync.Map
	publishInfoTable sync.Map
	subscribeTable   sync.Map
}

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
						if _, ok := bd.brokerAddrs[1]; ok {
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

func (m *TopicRouteInfoManager) GetTopicPublishInfo(topic string) *TopicPublishInfo {
	if v, ok := m.publishInfoTable.Load(topic); ok {
		return v.(*TopicPublishInfo)
	}
	return nil
}

func (t *TopicPublishInfo) GetQueueNumber() int {
	return len(t.MsgQueues)
}

func (t *TopicPublishInfo) GetWriteQueueNumber(brokerName string) int {
	for _, queueData := range t.topicRoute.queueDatas {
		if queueData.BrokerName == brokerName {
			return queueData.WriteQueueNums
		}
	}
	return -1
}

func (t *TopicPublishInfo) GetNextQueue() MessageQueue {
	nextIndex := atomic.AddInt32(&t.nextQueueIndex, 1)
	pos := int(nextIndex) % len(t.MsgQueues)
	if pos < 0 {
		pos = 0
	}
	return t.MsgQueues[pos]
}

func GetTopicSubscribeInfo() {

}

func GetBrokerAddrByName(brokerName string, vipPrefer bool) string {
	return ""
}
