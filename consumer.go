package mqclient

import (
	"errors"
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
	consumerGroup      string
	config             ConsumerConfig
	status             ServiceState
	instanceName       string
	mqClient           *MQClient
	mqAllocateStrategy MQAllocateStrategy
}

//NewPushConsumer returns a default rocketmq push mode consumer
func NewPushConsumer(consumerGroup string, config *ConsumerConfig) (PushConsumer, error) {
	if consumerGroup == "" || config == nil {
		return nil, errors.New("consumerGroup and config must not be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	c := &defaultPushConsumer{
		consumerGroup: consumerGroup,
		config:        *config,
	}
	return c, nil
}

func (c *defaultPushConsumer) Start() error {
	return nil
}

func (c *defaultPushConsumer) Shutdown() {

}

func (c *defaultPushConsumer) Subscribe(topic string, subExpression string) error {
	return nil
}

func (c *defaultPushConsumer) Unsubscirbe(topic string) {

}

func (c *defaultPushConsumer) RegisterConcurrentlyHandler(handler MessageConcurrentlyHandler) {

}

func (c *defaultPushConsumer) RegisterOrderlyHandler(handler MessageOrderlyHandler) {

}

//NewPullConsumer returns a default rocketmq pull mode consumer
func NewPullConsumer() PullConsumer {
	return nil
}

//GetAllSubscribedTopics returns topic had been subscribe by consumer
func GetAllSubscribedTopics() []string {
	return []string{}
}
