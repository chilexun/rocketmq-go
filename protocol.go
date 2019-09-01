package mqclient

import "encoding/json"

type RequestCode int

const (
	SEND_MESSAGE RequestCode = iota + 10
	PULL_MESSAGE
	QUERY_MESSAGE
	QUERY_BROKER_OFFSET
	QUERY_CONSUMER_OFFSET
	UPDATE_CONSUMER_OFFSET
	UPDATE_AND_CREATE_TOPIC RequestCode = 17
	GET_ALL_TOPIC_CONFIG    RequestCode = iota + 21
	GET_TOPIC_CONFIG_LIST
	GET_TOPIC_NAME_LIST
	_
	UPDATE_BROKER_CONFIG
	GET_BROKER_CONFIG
	TRIGGER_DELETE_FILES
	GET_BROKER_RUNTIME_INFO
	SEARCH_OFFSET_BY_TIMESTAMP
	GET_MAX_OFFSET
	GET_MIN_OFFSET
	GET_EARLIEST_MSG_STORETIME
	VIEW_MESSAGE_BY_ID
	HEART_BEAT
	UNREGISTER_CLIENT
	CONSUMER_SEND_MSG_BACK
	END_TRANSACTION
	GET_CONSUMER_LIST_BY_GROUP
	CHECK_TRANSACTION_STATE
	NOTIFY_CONSUMER_IDS_CHANGED
)
const (
	PUT_KV_CONFIG RequestCode = iota + 100
	GET_KV_CONFIG
	DELETE_KV_CONFIG
	REGISTER_BROKER
	UNREGISTER_BROKER
	GET_ROUTEINTO_BY_TOPIC
	GET_BROKER_CLUSTER_INFO
)

type ResponseCode int

const (
	SUCCESS ResponseCode = iota
	SYSTEM_ERROR
	SYSTEM_BUSY
	REQUEST_CODE_NOT_SUPPORTED
	TRANSACTION_FAILED
	_
	_
	_
	_
	_
	FLUSH_DISK_TIMEOUT
	SLAVE_NOT_AVAILABLE
	FLUSH_SLAVE_TIMEOUT
	MESSAGE_ILLEGAL
	SERVICE_NOT_AVAILABLE
	VERSION_NOT_SUPPORTED
	NO_PERMISSION
	TOPIC_NOT_EXIST
	TOPIC_EXIST_ALREADY
	PULL_NOT_FOUND
	PULL_RETRY_IMMEDIATELY
	PULL_OFFSET_MOVED
	QUERY_NOT_FOUND
	SUBSCRIPTION_PARSE_FAILED
	SUBSCRIPTION_NOT_EXIST
	SUBSCRIPTION_NOT_LATEST
	SUBSCRIPTION_GROUP_NOT_EXIST
	FILTER_DATA_NOT_EXIST
	FILTER_DATA_NOT_LATEST
)
const (
	TRANSACTION_SHOULD_COMMIT ResponseCode = iota + 200
	TRANSACTION_SHOULD_ROLLBACK
	TRANSACTION_STATE_UNKNOW
	TRANSACTION_STATE_GROUP_WRONG
	NO_BUYER_ID
	NOT_IN_CURRENT_UNIT
	CONSUMER_NOT_ONLINE
	CONSUME_MSG_TIMEOUT
	NO_MESSAGE
)

type MsgSysFlag int

const (
	COMPRESSED_FLAG           MsgSysFlag = 0x1
	MULTI_TAGS_FLAG           MsgSysFlag = 0x1 << 1
	TRANSACTION_NOT_TYPE      MsgSysFlag = 0
	TRANSACTION_PREPARED_TYPE MsgSysFlag = 0x1 << 2
	TRANSACTION_COMMIT_TYPE   MsgSysFlag = 0x2 << 2
	TRANSACTION_ROLLBACK_TYPE MsgSysFlag = 0x3 << 2
)

type SerializeType byte

const (
	SerialTypeJson     SerializeType = 0
	SerialTypeRocketMQ SerializeType = 1
)

var CmdHeaderCodecs = [2]CmdHeaderCodec{
	new(jsonCodec),
	new(rocketMQCodec),
}

type CmdHeaderCodec interface {
	Encode(cmd *Command) ([]byte, error)
	Decode([]byte) (*Command, error)
}

type jsonCodec struct {
}

func (c *jsonCodec) Encode(cmd *Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func (c *jsonCodec) Decode(b []byte) (cmd *Command, err error) {
	cmd = new(Command)
	err = json.Unmarshal(b, cmd)
	return
}

type rocketMQCodec struct {
}

func (c *rocketMQCodec) Encode(cmd *Command) ([]byte, error) {
	return nil, nil
}

func (c *rocketMQCodec) Decode(b []byte) (*Command, error) {
	return nil, nil
}

type ProducerData struct {
	groupName string
}

type ConsumerType struct {
	typeCN string
}

type MessageModel struct {
	modeCN string
}

type ConsumeFromWhere string

const (
	CONSUME_FROM_LAST_OFFSET  ConsumeFromWhere = "CONSUME_FROM_LAST_OFFSET"
	CONSUME_FROM_FIRST_OFFSET ConsumeFromWhere = "CONSUME_FROM_FIRST_OFFSET"
	CONSUME_FROM_TIMESTAMP    ConsumeFromWhere = "CONSUME_FROM_TIMESTAMP"
)

type ExpressionType string

const (
	SQL92 ExpressionType = "SQL92"
	TAG   ExpressionType = "TAG"
)

type SubscriptionData struct {
	classFilterMode bool
	topic           string
	subString       string
	tagsSet         []string
	codeSet         []int
	subVersion      int64
	expressionType  ExpressionType
}

type ConsumerData struct {
	groupName           string
	consumeType         ConsumerType
	messageModel        MessageModel
	consumeFromWhere    ConsumeFromWhere
	subscriptionDataSet []SubscriptionData
	unitMode            bool
}

type HeartbeatData struct {
	clientID        string
	producerDataSet []ProducerData
	consumerDataSet []ConsumerData
}

type Permission int

const (
	PERM_PRIORITY Permission = 1 << 3
	PERM_READ     Permission = 1 << 2
	PERM_WRITE    Permission = 1 << 1
	PERM_INHERIT  Permission = 1 << 0
)

func PermitRead(perm Permission) bool {
	return (perm & PERM_READ) == PERM_READ
}

func PermitWrite(perm Permission) bool {
	return (perm & PERM_WRITE) == PERM_WRITE
}

func PermitInherit(perm Permission) bool {
	return (perm & PERM_INHERIT) == PERM_INHERIT
}
