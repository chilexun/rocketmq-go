package mqclient

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

type MessageQueue struct {
	Topic      string
	BrokerName string
	QueueId    int
}

type SendStats struct {
	BrokerName string
	latency    time.Duration
	isFail     bool
}

type MQSelectStrategy interface {
	SelectOneMessageQueue(topicInfo *TopicPublishInfo, lastBrokerName string) MessageQueue
	UpdateSendStats(stats SendStats)
}

var latencyMax = [7]time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 550 * time.Millisecond,
	1000 * time.Millisecond, 2000 * time.Millisecond, 3000 * time.Millisecond, 15000 * time.Millisecond}
var notAvailable = [7]time.Duration{0 * time.Millisecond, 0 * time.Millisecond, 30000 * time.Millisecond,
	60000 * time.Millisecond, 120000 * time.Millisecond, 180000 * time.Millisecond, 600000 * time.Millisecond}

type LatencyFaultStrategy struct {
	latencyTable sync.Map
}

type latencyItem struct {
	brokerName string
	started    time.Time
	latency    time.Duration
}

func NewStrategy() MQSelectStrategy {
	return &LatencyFaultStrategy{}
}

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
			mq.QueueId = rand.Intn(writeQueueNums)
			return mq
		} else {
			s.latencyTable.Delete(notBestBroker)
		}
	}
	return topicInfo.GetNextQueue()
}

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
	return time.Since(i.started) > i.latency
}
