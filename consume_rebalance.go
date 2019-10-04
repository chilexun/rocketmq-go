package mqclient

import (
	"sort"
	"sync"
)

type allocatedMQChangedListener interface {
	onMQAppend(mq MessageQueue)
	onMQRemove(mq MessageQueue)
}

type rebalancer struct {
	consumerGroup      string
	config             *ConsumerConfig
	client             *MQClient
	mqAllocateStrategy MQAllocateStrategy
	listeners          []allocatedMQChangedListener
	processQueues      sync.Map //Key: topic, value:[]messageQueue
}

//newRebalancer returns new rebalancer for specified consumer
func newRebalancer(consumerGroup string, config *ConsumerConfig, mqclient *MQClient) *rebalancer {
	return &rebalancer{
		consumerGroup:      consumerGroup,
		config:             config,
		client:             mqclient,
		mqAllocateStrategy: mqAllocateStratgies[config.MQAllocateStrategy],
	}
}

func (r *rebalancer) DoRebalance() {
	topics := r.client.GetAllSubscribeTopics()
	for _, topic := range topics {
		r.doRebalance(topic)
	}
}

func (r *rebalancer) doRebalance(topic string) {
	mqs := r.client.GetTopicSubscribeInfo(topic)
	if len(mqs) == 0 {
		logger.Warnf("doRebalance, %s, but the topic[%s] not exist.", r.consumerGroup, topic)
		return
	}
	mqAll := make([]MessageQueue, len(mqs))
	copy(mqAll, mqs)
	if r.config.MessageModel == ConsumeCluster {
		mqAll = r.allocateMQs(topic, mqAll)
	}

	var rawMQs []MessageQueue
	if value, ok := r.processQueues.Load(topic); ok {
		rawMQs = value.([]MessageQueue)
	}
	add, rem := resovleChangedQueues(mqAll, rawMQs)
	r.processQueues.Store(topic, mqAll)

	defer func() {
		if p := recover(); p != nil {
			logger.Errorf("DoRebalance: listener process error - %s", p)
		}
	}()
	for _, mq := range add {
		for _, listener := range r.listeners {
			listener.onMQAppend(mq)
		}
	}
	for _, mq := range rem {
		for _, listener := range r.listeners {
			listener.onMQRemove(mq)
		}
	}
}

func (r *rebalancer) allocateMQs(topic string, mqAll []MessageQueue) []MessageQueue {
	cidAll := r.client.FindConsumerIDs(topic, r.consumerGroup)
	if len(cidAll) <= 0 {
		logger.Warnf("doRebalance, %s topic[%s], get consumer id list failed", r.consumerGroup, topic)
	}

	sort.SliceStable(mqAll, func(i, j int) bool {
		if mqAll[i].Topic != mqAll[j].Topic {
			return mqAll[i].Topic < mqAll[j].Topic
		}
		if mqAll[i].BrokerName != mqAll[j].BrokerName {
			return mqAll[i].BrokerName < mqAll[j].BrokerName
		}
		return mqAll[i].QueueID <= mqAll[j].QueueID
	})
	sort.SliceStable(cidAll, func(i, j int) bool {
		return cidAll[i] <= cidAll[j]
	})

	return r.mqAllocateStrategy.AllocateMessageQueues(r.consumerGroup, r.client.clientID, mqAll, cidAll)
}

func resovleChangedQueues(currentMQs []MessageQueue, rawMQs []MessageQueue) (appended, removed []MessageQueue) {
	return nil, nil
}

func (r *rebalancer) addChangeListener(listener allocatedMQChangedListener) {
	r.listeners = append(r.listeners, listener)
}
