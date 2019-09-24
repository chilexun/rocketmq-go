package mqclient

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//Message ext. properties name definition
const (
	MessageKeys                           string = "KEYS"
	MessageTags                           string = "TAGS"
	MessageWaitStoreMsgOK                 string = "WAIT"
	MessageDelayTimeLevel                 string = "DELAY"
	MessageRetryTopic                     string = "RETRY_TOPIC"
	MessageRealTopic                      string = "REAL_TOPIC"
	MessageRealQueueID                    string = "REAL_QID"
	MessageTransactionPrepared            string = "TRAN_MSG"
	MessageProducerGroup                  string = "PGROUP"
	MessageMinOffset                      string = "MIN_OFFSET"
	MessageMaxOffset                      string = "MAX_OFFSET"
	MessageBuyID                          string = "BUYER_ID"
	MessageOriginMessageID                string = "ORIGIN_MESSAGE_ID"
	MessageTransferFlag                   string = "TRANSFER_FLAG"
	MessageCorrectionFlag                 string = "CORRECTION_FLAG"
	MessageMQ2Flag                        string = "MQ2_FLAG"
	MessageReconsumeTime                  string = "RECONSUME_TIME"
	MessageMsgRegion                      string = "MSG_REGION"
	MessageTraceSwitch                    string = "TRACE_ON"
	MessageUniqClientMessageIDKeyIdx      string = "UNIQ_KEY"
	MessageMaxReconsumeTimes              string = "MAX_RECONSUME_TIMES"
	MessageConsumeStartTimestamp          string = "CONSUME_START_TIME"
	MessageTransactionPreparedQueueOffset string = "TRAN_PREPARED_QUEUE_OFFSET"
	MessageTransactionCheckTimes          string = "TRANSACTION_CHECK_TIMES"
	MessageCheckImmunityTimeInSeconds     string = "CHECK_IMMUNITY_TIME_IN_SECONDS"

	MessageKeySeparator       string = " "
	MessageNameValueSeparator byte   = 1
	MessagePropertySeparator  byte   = 2
)

const (
	//AutoCreateTopicKeyTopic will be created at broker when isAutoCreateTopicEnable
	AutoCreateTopicKeyTopic = "TBW102"
	//MaxMessageSize is the allowed message size in bytes, default 4M
	MaxMessageSize = 1024 * 1024 * 4
)

var fixMsgIDPrefix string
var startTime time.Time
var nextStartTime time.Time
var mutex sync.Mutex
var counter int32

var validTopic = regexp.MustCompile(`^[%|a-zA-Z0-9_-]{1,255}$`)

func init() {
	buf := new(bytes.Buffer) //length: 4+2+4
	buf.Write(GetIPAddr())
	binary.Write(buf, binary.BigEndian, int16(os.Getpid()))
	binary.Write(buf, binary.BigEndian, rand.Int31())
	fixMsgIDPrefix = strings.ToUpper(hex.EncodeToString(buf.Bytes()))
	resetStartTime()
}

func resetStartTime() {
	mutex.Lock()
	defer mutex.Unlock()
	now := time.Now()
	if !now.After(nextStartTime) {
		return
	}
	startTime = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.Local)
	nextStartTime = startTime.AddDate(0, 1, 0)
}

//Message represents a message from producer to broker
type Message struct {
	Topic         string
	Flag          int
	Properties    map[string]string
	Body          []byte
	TransactionID string
}

//NewMessage create a message entity, the default value of WaitStoreMsgOK is true
func NewMessage(topic string, body []byte) Message {
	props := make(map[string]string, 1)
	props[MessageWaitStoreMsgOK] = "true"
	return Message{
		Topic:      topic,
		Flag:       0,
		Properties: props,
		Body:       body,
	}
}

//SetUniqID set a new id for message
func (m *Message) SetUniqID(id string) {
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	m.Properties[MessageUniqClientMessageIDKeyIdx] = id
}

//GetUniqID returns the message uniq id
func (m *Message) GetUniqID() string {
	if m.Properties == nil {
		return ""
	}
	return m.Properties[MessageUniqClientMessageIDKeyIdx]
}

//GenerateUniqMsgID return a uniq message ID
func GenerateUniqMsgID() string {
	var builder strings.Builder
	builder.WriteString(fixMsgIDPrefix)
	buf := new(bytes.Buffer)
	if time.Now().After(nextStartTime) {
		resetStartTime()
	}
	gap := time.Since(startTime).Nanoseconds() / 1e6
	binary.Write(buf, binary.BigEndian, int32(gap))
	binary.Write(buf, binary.BigEndian, int16(atomic.AddInt32(&counter, 1)))
	builder.WriteString(strings.ToUpper(hex.EncodeToString(buf.Bytes())))
	return builder.String()
}

//ValidateMsg check the msg props
func ValidateMsg(m *Message) error {
	if !validTopic.MatchString(m.Topic) {
		return fmt.Errorf("The specified topic[%s] contains illegal characters or has invalid length", m.Topic)
	}
	if AutoCreateTopicKeyTopic == m.Topic {
		return fmt.Errorf("The topic[%s] is conflict with AUTO_CREATE_TOPIC_KEY_TOPIC", m.Topic)
	}
	if m.Body == nil || len(m.Body) == 0 || len(m.Body) > MaxMessageSize {
		return errors.New("The message body is nil or exceed the max allowed length")
	}
	return nil
}
