package mqclient

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

//PullStatus specifies the result of Pull
type PullStatus int

//PullStatus values
const (
	PullFound PullStatus = iota
	PullNoNewMessage
	PullNoMatchedMessage
	PullOffsetIllegal
)

//MessageConcurrentlyHandler will be callback when message received and the message is not orderly
type MessageConcurrentlyHandler func([]MessageExt, ConsumeConcurrentlyContext)

//MessageOrderlyHandler will be callback when orderly message is received
type MessageOrderlyHandler func([]MessageExt, ConsumeOrderlyContext)

//ConsumeConcurrentlyContext represents the context of concurrently consume
type ConsumeConcurrentlyContext struct {
	MsgQueue                  MessageQueue
	DelayLevelWhenNextConsume int
	AckIndex                  int
}

//ConsumeOrderlyContext represents the context of orderly consume
type ConsumeOrderlyContext struct {
	MsgQueue                      MessageQueue
	AutoCommit                    bool
	SuspendCurrentQueueTimeMillis time.Duration
}

//PullCallback is async pull callback handler
type PullCallback interface {
	OnSuccess(result PullResult)
	OnError(err error)
}

//PullResult represents the result data of pull message
type PullResult struct {
	PullStatus      PullStatus
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	MsgFoundList    []MessageExt
}

//MQChangedHandler will be callback when a message queue changed
type MQChangedHandler func(topic string, mqAll, mqDivided []MessageQueue)

//Consumer contains methods to subscribe message from MQ
type Consumer interface {
	Start() error
	Shutdown()
}

//PushConsumer keep connection with broker and wait for messages
type PushConsumer interface {
	Consumer
	Subscribe(topic string, subExpression string) error
	Unsubscirbe(topic string)
	RegisterConcurrentlyHandler(handler MessageConcurrentlyHandler)
	RegisterOrderlyHandler(handler MessageOrderlyHandler)
}

//PullConsumer fetch messages from brokers on specified queue
type PullConsumer interface {
	Consumer
	Pull(mq MessageQueue, subExpression string, offset int64, maxNums int64, timeout time.Duration) error
	PullAsync(mq MessageQueue, subExpression string, offset int64, maxNums int64, callback PullCallback, timeout time.Duration) error
	RegisterMQChangedHandler(topic string, handler MQChangedHandler)
}

type defaultPushConsumer struct {
	consumerGroup       string
	config              ConsumerConfig
	status              ServiceState
	instanceName        string
	mqClient            *MQClient
	mqAllocateStrategy  MQAllocateStrategy
	concurrentlyHandler MessageConcurrentlyHandler
	orderlyHandler      MessageOrderlyHandler
	subscription        map[string]string
}

var consumerTable sync.Map

//NewPushConsumer returns a default rocketmq push mode consumer
func NewPushConsumer(consumerGroup string, config *ConsumerConfig) (PushConsumer, error) {
	if consumerGroup == "" || config == nil {
		return nil, errors.New("consumerGroup and config must not be nil")
	}
	if !validGroupName.MatchString(consumerGroup) || consumerGroup == "DEFAULT_CONSUMER" {
		return nil, errors.New("the specified consumer group is invalid")
	}
	if config.ConsumeTimestamp.IsZero() {
		config.ConsumeTimestamp = time.Now().Add(-30 * time.Minute)
	}
	instanceName := config.InstanceName
	if instanceName == "" {
		instanceName = DefaultClientInstanceName
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	c := &defaultPushConsumer{
		consumerGroup:      consumerGroup,
		instanceName:       instanceName,
		config:             *config,
		status:             CreateJust,
		mqAllocateStrategy: mqAllocateStratgies[config.MQAllocateStrategy],
	}
	return c, nil
}

func (c *defaultPushConsumer) Start() error {
	if !atomic.CompareAndSwapInt32(&c.status, CreateJust, StartFailed) {
		return errors.New("The consumer service state not OK, maybe started once")
	}
	if c.concurrentlyHandler == nil && c.orderlyHandler == nil {
		return errors.New("At least one message handler should be specified")
	}
	if c.config.MessageModel == ConsumeCluster && c.instanceName == DefaultClientInstanceName {
		c.instanceName = strconv.Itoa(os.Getpid())
	}
	if !registerConsumer(c.consumerGroup, c) {
		atomic.StoreInt32(&c.status, CreateJust)
		return errors.New("The consumer group has been created before")
	}

	c.mqClient = GetClientInstance(&c.config.ClientConfig, c.instanceName)
	err := c.mqClient.Start()
	if err != nil {
		unregisterConsumer(c.consumerGroup)
		return err
	}
	atomic.StoreInt32(&c.status, Running)
	c.mqClient.IncrReference()
	c.mqClient.SendHeartbeatToBrokers()
	return nil
}

func (c *defaultPushConsumer) Shutdown() {
	if atomic.LoadInt32(&c.status) == Running {
		unregisterConsumer(c.consumerGroup)
		c.mqClient.DecrReference()
		c.mqClient.Shutdown()
		atomic.StoreInt32(&c.status, ShutdownAlready)
	}
}

func (c *defaultPushConsumer) Subscribe(topic string, subExpression string) error {
	c.subscription[topic] = subExpression
	return nil
}

func (c *defaultPushConsumer) Unsubscirbe(topic string) {
	delete(c.subscription, topic)
}

func (c *defaultPushConsumer) RegisterConcurrentlyHandler(handler MessageConcurrentlyHandler) {
	c.concurrentlyHandler = handler
}

func (c *defaultPushConsumer) RegisterOrderlyHandler(handler MessageOrderlyHandler) {
	c.orderlyHandler = handler
}

//register producer if not exist, one producer instance for each producerGroup
//return true if register success
func registerConsumer(consumerGroup string, consumer Consumer) bool {
	_, loaded := consumerTable.LoadOrStore(consumerGroup, consumer)
	return !loaded
}

func unregisterConsumer(consumerGroup string) {
	consumerTable.Delete(consumerGroup)
}

//NewPullConsumer returns a default rocketmq pull mode consumer
func NewPullConsumer() PullConsumer {
	return nil
}

//GetAllSubscribedTopics returns topic had been subscribe by consumer
func GetAllSubscribedTopics() []string {
	return []string{}
}
