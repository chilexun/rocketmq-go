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

var producerTable sync.Map

//SendStatus is the enum type of send message result
type SendStatus int

//Send status definition
const (
	SendOK SendStatus = iota
	SendFail
	BrokerFlushDiskTimeout
	BrokerFlushSlaveTimeout
	BrokerSlaveNotAvailable
)

//SendResult represents the result of send message
type SendResult struct {
	SendStatus    SendStatus
	MsgID         string
	MsgQueue      MessageQueue
	QueueOffset   int64
	TransactionID string
	OffsetMsgID   string
	RegionID      string
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
	config           ProducerConfig
	status           ServiceState
	instanceName     string
	mqClient         *MQClient
	mqSelectStrategy MQSelectStrategy
}

//NewProducer returns default rocketmq producer client
func NewProducer(producerGroup string, config *ProducerConfig) (Producer, error) {
	if producerGroup == "" || config == nil {
		return nil, errors.New("producerGroup and config must not be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	instanceName := config.InstanceName
	if instanceName == "" {
		instanceName = DefaultClientInstanceName
	}
	p := &defaultProducer{
		producerGroup:    producerGroup,
		config:           *config,
		status:           CreateJust,
		instanceName:     instanceName,
		mqSelectStrategy: NewStrategy(),
	}
	return p, nil
}

func (p *defaultProducer) Start() error {
	if !atomic.CompareAndSwapInt32(&p.status, CreateJust, StartFailed) {
		return errors.New("The producer service state not OK, maybe started once")
	}
	if p.producerGroup != ClientInnerProducerGroup && p.instanceName == DefaultClientInstanceName {
		p.instanceName = strconv.Itoa(os.Getpid())
	}
	if !registerProducer(p.producerGroup, p) {
		atomic.StoreInt32(&p.status, CreateJust)
		return errors.New("The producer group has been created before")
	}

	p.mqClient = GetClientInstance(&p.config.ClientConfig, p.instanceName)
	err := p.mqClient.Start()
	if err != nil {
		unregisterProducer(p.producerGroup)
		return err
	}

	atomic.StoreInt32(&p.status, Running)
	p.mqClient.IncrReference()
	p.mqClient.SendHeartbeatToBrokers()
	return nil
}

func (p *defaultProducer) Shutdown() {
	if atomic.LoadInt32(&p.status) == Running {
		unregisterProducer(p.producerGroup)
		p.mqClient.DecrReference()
		p.mqClient.Shutdown()
		atomic.StoreInt32(&p.status, ShutdownAlready)
	}
}

func (p *defaultProducer) Send(msg Message, timeout time.Duration) (SendResult, error) {
	failResult := SendResult{SendStatus: SendFail}
	if atomic.LoadInt32(&p.status) != Running {
		return failResult, errors.New("The producer service state not OK")
	}
	if err := ValidateMsg(&msg); err != nil {
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
			if sendResult.SendStatus != SendOK {
				if p.config.RetryAnotherBrokerWhenNotStoreOK {
					continue
				}
			}
			return *sendResult, nil
		}

		p.mqSelectStrategy.UpdateSendStats(SendStats{mq.BrokerName, time.Since(beginTime), true})
		switch err.(type) {
		case *MQBrokerError:
			brokerErr := err.(MQBrokerError)
			switch brokerErr.Code {
			case TopicNotExist, ServiceNotAvailable, SystemError, NoPermission, NoBuyeID, NotInCurrentUnit:
				continue
			default:
				if sendResult != nil {
					return *sendResult, nil
				}
			}
		default:
			continue
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

	if msg.GetUniqID() == "" {
		msg.SetUniqID(GenerateUniqMsgID())
	}
	var sysFlag MsgSysFlag
	if len(msg.Body) >= p.config.CompressMsgBodyOverHowmuch {
		compressBody, err := Compress(msg.Body, p.config.ZipCompressLevel)
		if err == nil {
			defer func(rawBody []byte) { msg.Body = rawBody }(msg.Body)
			msg.Body = compressBody
			sysFlag |= CompressedFlag
		} else {
			logger.Warn("Compress message body error:" + err.Error())
		}
	}
	if msg.Properties[MessageTransactionPrepared] == "true" {
		sysFlag |= TransactionPreparedType
	}
	request := &SendMessageRequest{
		ProducerGroup:  p.producerGroup,
		Topic:          mq.Topic,
		QueueID:        mq.QueueId,
		SysFlag:        sysFlag,
		BornTimestamp:  time.Now().UnixNano() / 1e6,
		Flag:           msg.Flag,
		Properties:     msg.Properties,
		ReconsumeTimes: 0,
		Body:           msg.Body,
	}
	if strings.HasPrefix(request.Topic, "%RETRY%") {
		setRetryHeader(request, msg.Properties)
	}

	// cmd := SendMessage(requestHeader, msg.Body)
	response, err := p.mqClient.SendMessageRequest(brokerAddr, mq, request, timeout)
	if err != nil {
		return nil, err
	}

	sendResult := new(SendResult)
	switch code := response.Code; code {
	case Success:
		sendResult.SendStatus = SendOK
	case FlushDiskTimeout:
		sendResult.SendStatus = BrokerFlushDiskTimeout
	case FlushSlaveTimeout:
		sendResult.SendStatus = BrokerFlushSlaveTimeout
	case SlaveNotAvailable:
		sendResult.SendStatus = BrokerSlaveNotAvailable
	default:
		return nil, MQBrokerError{code, response.Remark}
	}

	sendResult.MsgID = msg.GetUniqID()
	sendResult.OffsetMsgID = response.MsgID
	sendResult.MsgQueue = MessageQueue{mq.Topic, mq.BrokerName, response.QueueID}
	sendResult.QueueOffset = response.QueueOffset
	sendResult.TransactionID = response.TransactionID
	return sendResult, nil
}

func setRetryHeader(header *SendMessageRequest, msgProps map[string]string) {
	if reconsumeTimes := msgProps[MessageReconsumeTime]; reconsumeTimes != "" {
		header.ReconsumeTimes, _ = strconv.Atoi(reconsumeTimes)
		delete(msgProps, MessageReconsumeTime)
	}
	if maxReTimes := msgProps[MessageMaxReconsumeTimes]; maxReTimes != "" {
		header.MaxReconsumeTimes, _ = strconv.Atoi(maxReTimes)
		delete(msgProps, MessageMaxReconsumeTimes)
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

//GetAllProducerGroups returns the snapshot of all register producer group
func GetAllProducerGroups() []string {
	groups := make([]string, 0)
	producerTable.Range(func(k interface{}, v interface{}) bool {
		groups = append(groups, k.(string))
		return true
	})
	return groups
}
