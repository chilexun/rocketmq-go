package mqclient

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

//SendStats represents a send request statistic
type SendStats struct {
	BrokerName string
	latency    time.Duration
	isFail     bool
}

//MQSelectStrategy is the strategy of select which queue to send message
type MQSelectStrategy interface {
	SelectOneMessageQueue(topicInfo *TopicPublishInfo, lastBrokerName string) MessageQueue
	UpdateSendStats(stats SendStats)
}

var latencyMax = [7]time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 550 * time.Millisecond,
	1000 * time.Millisecond, 2000 * time.Millisecond, 3000 * time.Millisecond, 15000 * time.Millisecond}
var notAvailable = [7]time.Duration{0 * time.Millisecond, 0 * time.Millisecond, 30000 * time.Millisecond,
	60000 * time.Millisecond, 120000 * time.Millisecond, 180000 * time.Millisecond, 600000 * time.Millisecond}

//LatencyFaultStrategy is a strategy which consider send latency
type LatencyFaultStrategy struct {
	latencyTable sync.Map
}

type latencyItem struct {
	brokerName string
	started    time.Time
	latency    time.Duration
}

//NewSelectStrategy returns a strategy of select mq when send msg
func NewSelectStrategy() MQSelectStrategy {
	return &LatencyFaultStrategy{}
}

//SelectOneMessageQueue returns the queue selected
func (s *LatencyFaultStrategy) SelectOneMessageQueue(topicInfo *TopicPublishInfo, lastBrokerName string) MessageQueue {
	queueNumber := topicInfo.GetQueueNumber()
	for i := 0; i < queueNumber; i++ {
		mq := topicInfo.GetNextQueue()
		if s.isBrokerAvaliable(mq.BrokerName) && (lastBrokerName == "" || mq.BrokerName == lastBrokerName) {
			return mq
		}
	}

	notBestBroker := s.pickOneBroker()
	if notBestBroker != "" {
		writeQueueNums := topicInfo.GetWriteQueueNumber(notBestBroker)
		if writeQueueNums > 0 {
			mq := topicInfo.GetNextQueue()
			mq.BrokerName = notBestBroker
			mq.QueueID = rand.Intn(writeQueueNums)
			return mq
		}
		s.latencyTable.Delete(notBestBroker)
	}
	return topicInfo.GetNextQueue()
}

//UpdateSendStats set the queue statistics to strategy
func (s *LatencyFaultStrategy) UpdateSendStats(stats SendStats) {
	latency := stats.latency
	if stats.isFail {
		latency = 30 * time.Second
	}
	var invalidDuration time.Duration
	for i, max := range latencyMax {
		if latency >= max {
			invalidDuration = notAvailable[i]
		}
	}

	if item, ok := s.latencyTable.Load(stats.BrokerName); ok {
		item.(*latencyItem).started = time.Now()
		item.(*latencyItem).latency = invalidDuration
	} else {
		item := &latencyItem{
			stats.BrokerName,
			time.Now(),
			invalidDuration,
		}
		s.latencyTable.Store(stats.BrokerName, item)
	}
}

func (s *LatencyFaultStrategy) isBrokerAvaliable(brokerName string) bool {
	if item, ok := s.latencyTable.Load(brokerName); ok {
		return item.(*latencyItem).isAvaliable()
	}
	return true
}

func (s *LatencyFaultStrategy) pickOneBroker() string {
	items := make([]*latencyItem, 0, 16)
	s.latencyTable.Range(func(key interface{}, v interface{}) bool {
		items = append(items, v.(*latencyItem))
		return true
	})
	if len(items) == 0 {
		return ""
	}

	sort.SliceStable(items, func(i, j int) bool {
		valid1, valid2 := items[i].isAvaliable(), items[j].isAvaliable()
		if valid1 != valid2 {
			if valid1 {
				return true
			} else if valid2 {
				return false
			}
		}
		if items[i].latency != items[j].latency {
			return (items[i].latency - items[j].latency) < 0
		}
		if items[i].started != items[j].started {
			return items[i].started.Before(items[j].started)
		}
		return false
	})

	half := len(items) / 2
	if half == 0 {
		return items[0].brokerName
	}
	return items[rand.Intn(half)].brokerName
}

func (i *latencyItem) isAvaliable() bool {
	return i.latency == 0 || time.Since(i.started) > i.latency
}

//MQAllocateStrategyType specifies the supported allocated strategy
type MQAllocateStrategyType int

//allocate strategy names
const (
	MQAllocateAVG MQAllocateStrategyType = iota
	MQAllocateAVGByCircle
	MQAllocateConfig
	MQAllocateMachineRoom
	MQAllocateConsistentHash
)

var mqAllocateStratgies = map[MQAllocateStrategyType]MQAllocateStrategy{}

//MQAllocateStrategy is the strategy of allocate mqs to consumers
type MQAllocateStrategy interface {
	AllocateMessageQueues(consumerGroup string, cid string, mqall []MessageQueue, cidAll []string) []MessageQueue
}
