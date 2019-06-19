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
	OrderTopic   bool
	HasRouteInfo bool
	MsgQueues    []MessageQueue
	topicRoute   *TopicRouteData
}

var topicRouteTable sync.Map
var brokerAddrTable sync.Map
var publishInfoTable sync.Map
var subscribeTable sync.Map
var topicRouteLock uint32

func SetTopicRouteData(topic string, data *TopicRouteData) {
	if !atomic.CompareAndSwapUint32(&topicRouteLock, 0, 1) {
		//log SetTopicRouteData tryLock timeout
	}
	defer atomic.StoreUint32(&topicRouteLock, 0)

	for _, brokerData := range data.brokerDatas {
		brokerAddrTable.Store(brokerData.brokerName, brokerData.brokerAddrs)
	}
	publishInfoTable.Store(topic, toPublishInfo(topic, data))
	subscribeTable.Store(topic, toSubscribeInfo(topic, data))
	topicRouteTable.Store(topic, cloneRouteData(data))
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

func GetTopicPublishInfo(topic string) *TopicPublishInfo {
	return nil
}

func GetTopicSubscribeInfo() {

}

func GetBrokerAddrByName(brokerName string, vipPrefer bool) string {
	return ""
}
