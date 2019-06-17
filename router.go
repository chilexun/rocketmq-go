package mqclient

import "time"

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

type LatencyFaultStrategy struct {
}

func NewFaultStrateguy() MQSelectStrategy {
	return nil
}

func (s *LatencyFaultStrategy) SelectOneMessageQueue(topicInfo *TopicPublishInfo, lastBrokerName string) MessageQueue {
	return MessageQueue{}
}

func (s *LatencyFaultStrategy) UpdateSendStats(stats SendStats) {

}
