package mqclient

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PROPERTY_KEYS                              string = "KEYS"
	PROPERTY_TAGS                              string = "TAGS"
	PROPERTY_WAIT_STORE_MSG_OK                 string = "WAIT"
	PROPERTY_DELAY_TIME_LEVEL                  string = "DELAY"
	PROPERTY_RETRY_TOPIC                       string = "RETRY_TOPIC"
	PROPERTY_REAL_TOPIC                        string = "REAL_TOPIC"
	PROPERTY_REAL_QUEUE_ID                     string = "REAL_QID"
	PROPERTY_TRANSACTION_PREPARED              string = "TRAN_MSG"
	PROPERTY_PRODUCER_GROUP                    string = "PGROUP"
	PROPERTY_MIN_OFFSET                        string = "MIN_OFFSET"
	PROPERTY_MAX_OFFSET                        string = "MAX_OFFSET"
	PROPERTY_BUYER_ID                          string = "BUYER_ID"
	PROPERTY_ORIGIN_MESSAGE_ID                 string = "ORIGIN_MESSAGE_ID"
	PROPERTY_TRANSFER_FLAG                     string = "TRANSFER_FLAG"
	PROPERTY_CORRECTION_FLAG                   string = "CORRECTION_FLAG"
	PROPERTY_MQ2_FLAG                          string = "MQ2_FLAG"
	PROPERTY_RECONSUME_TIME                    string = "RECONSUME_TIME"
	PROPERTY_MSG_REGION                        string = "MSG_REGION"
	PROPERTY_TRACE_SWITCH                      string = "TRACE_ON"
	PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX     string = "UNIQ_KEY"
	PROPERTY_MAX_RECONSUME_TIMES               string = "MAX_RECONSUME_TIMES"
	PROPERTY_CONSUME_START_TIMESTAMP           string = "CONSUME_START_TIME"
	PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET string = "TRAN_PREPARED_QUEUE_OFFSET"
	PROPERTY_TRANSACTION_CHECK_TIMES           string = "TRANSACTION_CHECK_TIMES"
	PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS    string = "CHECK_IMMUNITY_TIME_IN_SECONDS"

	KEY_SEPARATOR string = " "

	NameValueSeparator byte = 1
	PropertySeparator  byte = 2
)

const (
	MsgIDLength = 4 + 2 + 4 + 4 + 2
)

var fixMsgIDPrefix string
var startTime time.Time
var nextStartTime time.Time
var mutex sync.Mutex
var counter int32

func init() {
	buf := bytes.NewBuffer(make([]byte, 10))
	ipv4, _ := GetIPv4(GetIPAddr())
	for _, b := range ipv4 {
		buf.WriteByte(b)
	}
	binary.Write(buf, binary.BigEndian, int16(os.Getpid()))
	binary.Write(buf, binary.BigEndian, rand.Int31())
	fixMsgIDPrefix = buf.String()
	resetStartTime()
}

func resetStartTime() {
	mutex.Lock()
	defer mutex.Unlock()
	now := time.Now()
	if !now.After(nextStartTime) {
		return
	}
	startTime = time.Date(now.Year(), now.Month(), 0, 0, 0, 0, 0, time.Local)
	nextStartTime = startTime.AddDate(0, 1, 0)
}

type Message struct {
	Topic         string
	Flag          int
	Properties    map[string]string
	Body          []byte
	TransactionId string
}

func NewMessage(topic string, body []byte) Message {
	props := make(map[string]string, 1)
	props[PROPERTY_WAIT_STORE_MSG_OK] = "true"
	return Message{
		Topic:      topic,
		Flag:       0,
		Properties: props,
		Body:       body,
	}
}

func (m *Message) Validate() error {
	return nil
}

func (m *Message) SetUniqID(id string) {
	m.Properties[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX] = id
}

func (m *Message) GetUniqID() string {
	return m.Properties[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX]
}

func GenerateUniqMsgID() string {
	var builder strings.Builder
	builder.WriteString(fixMsgIDPrefix)
	buf := bytes.NewBuffer(make([]byte, 4+2))
	if time.Now().After(nextStartTime) {
		resetStartTime()
	}
	gap := time.Now().Sub(startTime).Nanoseconds() / 1e6
	binary.Write(buf, binary.BigEndian, int32(gap))
	binary.Write(buf, binary.BigEndian, int16(atomic.AddInt32(&counter, 1)))
	builder.WriteString(hex.Dump(buf.Bytes()))
	return builder.String()
}
