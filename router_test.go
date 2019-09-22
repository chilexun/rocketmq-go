package mqclient

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestLatencyStrategy(t *testing.T) {
	strategy := NewStrategy()
	stats := SendStats{
		BrokerName: "testBroker",
		latency:    100 * time.Millisecond,
		isFail:     true,
	}
	strategy.UpdateSendStats(stats)
	impl := strategy.(*LatencyFaultStrategy)
	time.Sleep(1 * time.Minute)
	available := impl.isBrokerAvaliable("testBroker")
	if available {
		t.FailNow()
	}
	fmt.Println("Broker not availiable after last fail")

	stats.isFail = false
	strategy.UpdateSendStats(stats)
	available = impl.isBrokerAvaliable("testBroker")
	if !available {
		t.FailNow()
	}
	fmt.Println("Broker is availiable if lantency is 100ms")

	stats.latency = 600 * time.Millisecond
	strategy.UpdateSendStats(stats)
	if impl.isBrokerAvaliable("testBroker") {
		t.FailNow()
	}
	fmt.Println("Broker is temprary unavailiable if lantency is 600ms")
	time.Sleep(32 * time.Second)
	if !impl.isBrokerAvaliable("testBroker") {
		t.FailNow()
	}
	fmt.Println("Broker is availiable after 30s if lantency is 600ms")
}

func TestSelectOneMessageQueue(t *testing.T) {
	topicInfo := prepareTopicPublishInfo()
	strategy := NewStrategy()
	for i := 0; i < 10; i++ {
		mq := strategy.SelectOneMessageQueue(topicInfo, "testBroker0")
		if mq.BrokerName != "testBroker0" {
			t.Fatal("Changed to another broker")
		}
	}

	stats := SendStats{
		BrokerName: "testBroker0",
		latency:    2 * time.Second,
		isFail:     false,
	}
	strategy.UpdateSendStats(stats)

	stats.BrokerName = "testBroker1"
	stats.latency = 1 * time.Second
	strategy.UpdateSendStats(stats)

	stats.BrokerName = "testBroker2"
	stats.latency = 600 * time.Millisecond
	strategy.UpdateSendStats(stats)

	stats.BrokerName = "testBroker3"
	stats.latency = 10 * time.Millisecond
	strategy.UpdateSendStats(stats)

	for i := 0; i < 10; i++ {
		mq := strategy.SelectOneMessageQueue(topicInfo, "testBroker0")
		if mq.BrokerName != "testBroker2" && mq.BrokerName != "testBroker3" {
			t.Fatal("Get unavailiable broker:" + mq.BrokerName)
		}
	}

}

func prepareTopicPublishInfo() *TopicPublishInfo {
	topicInfo := new(TopicPublishInfo)
	queueData := make([]QueueData, 0)
	queues := make([]MessageQueue, 0)
	for i := 0; i < 4; i++ {
		broker := "testBroker" + strconv.Itoa(i)
		for j := 0; j < 4; j++ {
			mq := MessageQueue{
				Topic:      "testTopic",
				BrokerName: broker,
				QueueID:    j,
			}
			queues = append(queues, mq)
		}
		qd := QueueData{
			BrokerName:     broker,
			ReadQueueNums:  4,
			WriteQueueNums: 4,
			Perm:           PermWrite | PermRead,
		}
		queueData = append(queueData, qd)
	}
	topicInfo.MsgQueues = queues
	routeData := &TopicRouteData{
		queueDatas: queueData,
	}
	topicInfo.topicRoute = routeData
	return topicInfo
}
