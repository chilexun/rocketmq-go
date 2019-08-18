package mqclient

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CLIENT_INNER_PRODUCER_GROUP string = "CLIENT_INNER_PRODUCER_GROUP"
	DEFAULT_INSTANCE_NAME       string = "DEFAULT"
)

var producerTable sync.Map

type SendStatus int

const (
	SEND_OK SendStatus = iota
	SEND_FAIL
	BROKER_FLUSH_DISK_TIMEOUT
	BROKER_FLUSH_SLAVE_TIMEOUT
	BROKER_SLAVE_NOT_AVAILABLE
)

type SendResult struct {
	SendStatus    SendStatus
	MsgId         string
	MsgQueue      MessageQueue
	QueueOffset   int64
	TransactionId string
	OffsetMsgId   string
	RegionId      string
}

//Producer is a rocketmq producer client interface
type Producer interface {
	Start() error
	Shutdown()
	Send(msg Message, timeout time.Duration) (SendResult, error)
	SendOneway(msg Message, timeout time.Duration) (SendResult, error)
	SendAsync(msg Message, callback SendCallback, timeout time.Duration) error
}

//SendCallback is async request callback handler
type SendCallback interface {
	onSuccess(SendResult SendResult)
	onError(err error)
}

type defaultProducer struct {
	producerGroup    string
	config           Config
	status           ServiceState
	instanceName     string
	mqClient         *MQClient
	mqSelectStrategy MQSelectStrategy
}

//NewProducer returns default rocketmq producer client
func NewProducer(producerGroup string, config *Config) (Producer, error) {
	if producerGroup == "" || config == nil {
		return nil, errors.New("producerGroup and config must not be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	instanceName := config.InstanceName
	if instanceName == "" {
		instanceName = DEFAULT_INSTANCE_NAME
	}
	p := &defaultProducer{
		producerGroup:    producerGroup,
		config:           *config,
		status:           CREATE_JUST,
		instanceName:     instanceName,
		mqSelectStrategy: NewStrategy(),
	}
	return p, nil
}

func (p *defaultProducer) Start() error {
	if !atomic.CompareAndSwapInt32((*int32)(&p.status), int32(CREATE_JUST), int32(START_FAILED)) {
		return errors.New("The producer service state not OK, maybe started once")
	}
	if p.producerGroup != CLIENT_INNER_PRODUCER_GROUP && p.instanceName == DEFAULT_INSTANCE_NAME {
		p.instanceName = strconv.Itoa(os.Getpid())
	}
	if !registerProducer(p.producerGroup, p) {
		p.status = CREATE_JUST
		return errors.New("The producer group has been created before")
	}

	p.mqClient = GetClientInstance(&p.config, p.instanceName)
	err := p.mqClient.Start()
	if err != nil {
		unregisterProducer(p.producerGroup)
		return err
	}

	p.status = RUNNING
	p.mqClient.IncrReference()
	p.mqClient.SendHeartbeatToBrokers()
	return nil
}

func (p *defaultProducer) Shutdown() {
	if p.status == RUNNING {
		unregisterProducer(p.producerGroup)
		p.mqClient.DecrReference()
		p.mqClient.Shutdown()
		p.status = SHUTDOWN_ALREADY
	}
}

func (p *defaultProducer) Send(msg Message, timeout time.Duration) (SendResult, error) {
	failResult := SendResult{SendStatus: SEND_FAIL}
	if p.status != RUNNING {
		return failResult, errors.New("The producer service state not OK")
	}
	if err := msg.Validate(); err != nil {
		return failResult, err
	}
	beginTime := time.Now()
	topicPublishInfo := p.mqClient.GetTopicPublishInfo(msg.Topic)
	if topicPublishInfo == nil {
		return failResult, errors.New("No route info of this topic:" + msg.Topic)
	}

	maxRetry := p.config.RetryTimesWhenSendFailed
	var mq MessageQueue
	var sendResult *SendResult
	var err error
	for times := 0; times <= maxRetry; times++ {
		var lastBrokerName = mq.BrokerName
		mq = p.mqSelectStrategy.SelectOneMessageQueue(topicPublishInfo, lastBrokerName)
		sendResult, err = p.sendKernelImpl(&msg, &mq, topicPublishInfo, timeout)
		if err == nil {
			p.mqSelectStrategy.UpdateSendStats(SendStats{mq.BrokerName, time.Since(beginTime), false})
			if sendResult.SendStatus != SEND_OK {
				if p.config.RetryAnotherBrokerWhenNotStoreOK {
					continue
				}
			}
			return *sendResult, nil
		} else {
			p.mqSelectStrategy.UpdateSendStats(SendStats{mq.BrokerName, time.Since(beginTime), true})
			switch err.(type) {
			case MQBrokerError:
				brokerErr := err.(MQBrokerError)
				switch brokerErr.Code {
				case TOPIC_NOT_EXIST,
					SERVICE_NOT_AVAILABLE,
					SYSTEM_ERROR,
					NO_PERMISSION,
					NO_BUYER_ID,
					NOT_IN_CURRENT_UNIT:
					continue
				default:
					if sendResult != nil {
						return *sendResult, nil
					}
					break
				}
			default:
				continue
			}
		}
	}
	return failResult, err
}

func (p *defaultProducer) SendOneway(msg Message, timeout time.Duration) (SendResult, error) {
	return SendResult{}, nil
}

func (p *defaultProducer) SendAsync(msg Message, callback SendCallback, timeout time.Duration) error {
	return nil
}

func (p *defaultProducer) sendKernelImpl(msg *Message, mq *MessageQueue, topicInfo *TopicPublishInfo, timeout time.Duration) (*SendResult, error) {
	brokerAddr := GetBrokerAddrByName(mq.BrokerName, p.config.SendMessageWithVIPChannel)
	if brokerAddr == "" {
		return nil, errors.New(fmt.Sprint("The broker [s%] not exist", mq.BrokerName))
	}
	msg.SetUniqID()
	var rawBody []byte
	var sysFlag MsgSysFlag
	if len(msg.Body) >= p.config.CompressMsgBodyOverHowmuch {
		rawBody = msg.CompressBody(p.config.ZipCompressLevel)
		sysFlag |= COMPRESSED_FLAG
		defer func() { msg.Body = rawBody }()
	}
	if msg.Properties[PROPERTY_TRANSACTION_PREPARED] == "true" {
		sysFlag |= TRANSACTION_PREPARED_TYPE
	}
	requestHeader := &SendMessageRequestHeader{
		ProducerGroup:  p.producerGroup,
		Topic:          mq.Topic,
		QueueId:        mq.QueueId,
		SysFlag:        sysFlag,
		BornTimestamp:  time.Now().UnixNano() / 1e6,
		Flag:           msg.Flag,
		Properties:     msg.Properties2String(),
		ReconsumeTimes: 0,
	}
	if strings.HasPrefix(requestHeader.Topic, "%RETRY%") {
		setRetryHeader(requestHeader, msg.Properties)
	}

	cmd := SendMessage(requestHeader, msg.Body)
	response, err := p.mqClient.SendMessageRequest(brokerAddr, msg, mq, &cmd, timeout)
	if err != nil {
		return nil, err
	}

	sendResult := &SendResult{}
	switch code := ResponseCode(response.Code); code {
	case SUCCESS:
		sendResult.SendStatus = SEND_OK
	case FLUSH_DISK_TIMEOUT:
		sendResult.SendStatus = BROKER_FLUSH_DISK_TIMEOUT
	case FLUSH_SLAVE_TIMEOUT:
		sendResult.SendStatus = BROKER_FLUSH_SLAVE_TIMEOUT
	case SLAVE_NOT_AVAILABLE:
		sendResult.SendStatus = BROKER_SLAVE_NOT_AVAILABLE
	default:
		return nil, MQBrokerError{code, response.Remark}
	}
	respHeader := response.CustomHeader.(*SendMessageResponseHeader)
	sendResult.MsgId = msg.GetUniqID()
	sendResult.OffsetMsgId = respHeader.MsgId
	sendResult.MsgQueue = MessageQueue{mq.Topic, mq.BrokerName, respHeader.QueueId}
	sendResult.QueueOffset = respHeader.QueueOffset
	sendResult.TransactionId = respHeader.TransactionId
	return sendResult, nil
}

func setRetryHeader(header *SendMessageRequestHeader, msgProps map[string]string) {
	if reconsumeTimes := msgProps[PROPERTY_RECONSUME_TIME]; reconsumeTimes != "" {
		header.ReconsumeTimes, _ = strconv.Atoi(reconsumeTimes)
		delete(msgProps, PROPERTY_RECONSUME_TIME)
	}
	if maxReTimes := msgProps[PROPERTY_MAX_RECONSUME_TIMES]; maxReTimes != "" {
		header.MaxReconsumeTimes, _ = strconv.Atoi(maxReTimes)
		delete(msgProps, PROPERTY_MAX_RECONSUME_TIMES)
	}
}

//register producer if not exist, one producer instance for each producerGroup
//return true if register success
func registerProducer(producerGroup string, producer Producer) bool {
	_, loaded := producerTable.LoadOrStore(producerGroup, producer)
	return !loaded
}

func unregisterProducer(producerGroup string) {
	producerTable.Delete(producerGroup)
}

func GetAllProducerGroups() []string {
	groups := make([]string, 0)
	producerTable.Range(func(k interface{}, v interface{}) bool {
		groups = append(groups, k.(string))
		return true
	})
	return groups
}
