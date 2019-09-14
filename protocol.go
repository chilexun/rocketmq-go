package mqclient

import "encoding/json"

//RequestCode specifies the code of request
type RequestCode int

//Enum of request code
const (
	SendMessageReq RequestCode = iota + 10
	PullMessageReq
	QueryMessageReq
	QueryBrokerOffsetReq
	QueryConsumerOffsetReq
	UpdateConsumerOffsetReq
	UpdateAndCreateTopicReq RequestCode = 17
	GetAllTopicConfigReq    RequestCode = iota + 21
	GetTopicConfigListReq
	GetTopicNameListReq
	_
	UpdateBrokerConfigReq
	GetBrokerConfigReq
	TriggerDeleteFilesReq
	GetBrokerRuntimeInfoReq
	SearchOffsetByTimestampReq
	GetMaxOffsetReq
	GetMinOffsetReq
	GetEarliestMsgStoreTimeReq
	ViewMessageByIDReq
	HeartBeatReq
	UnRegisterClientReq
	ConsumerSendMsgBackReq
	EndTransactionReq
	GetConsumerListByGroupReq
	CheckTransactionStateReq
	NotifyConsumerIDsChangedReq
)

//Enum of KV request code
const (
	PutKVConfigReq RequestCode = iota + 100
	GetKVConfigReq
	DeleteKVConfigReq
	RegisterBrokerReq
	UnRegisterBrokerReq
	GetRouteInfoByTopicReq
	GetBrokerClusterInfoReq
)

//ResponseCode specifies the code of response
type ResponseCode int

//Enum of Response code
const (
	Success ResponseCode = iota
	SystemError
	SystemBusy
	RequestCodeNotSupported
	TransactionFailed
	_
	_
	_
	_
	_
	FlushDiskTimeout
	SlaveNotAvailable
	FlushSlaveTimeout
	MessageIllegal
	ServiceNotAvailable
	VersionNotSupported
	NoPermission
	TopicNotExist
	TopicExistAlready
	PullNotFound
	PullRetryImmediately
	PullOffsetMoved
	QueryNotFound
	SubscriptionParseFailed
	SubscriptionNotExist
	SubscriptionNotLatest
	SubscriptionGroupNotExist
	FilterDataNotExist
	FilterDataNotLastest
)

//Enum of transaction response code
const (
	TransactionShouldCommit ResponseCode = iota + 200
	TransactionShouldRollback
	TransactionStateUnknown
	TransactionStateGroupWrong
	NoBuyeID
	NotInCurrentUnit
	ConsumerNotOnline
	ConsumeMsgTimeout
	NoMessage
)

//MsgSysFlag specifies the sysflag of message
type MsgSysFlag int

//Enum of message flag
const (
	CompressedFlag          MsgSysFlag = 0x1
	MultiTagsFlag           MsgSysFlag = 0x1 << 1
	TransactionNotType      MsgSysFlag = 0
	TransactionPreparedType MsgSysFlag = 0x1 << 2
	TransactionCommitType   MsgSysFlag = 0x2 << 2
	TransactionRollbackType MsgSysFlag = 0x3 << 2
)

//SerializeType specifies the serial type of command
type SerializeType byte

//Enum of command serialize type
const (
	SerialTypeJson     SerializeType = 0
	SerialTypeRocketMQ SerializeType = 1
)

//CmdHeaderCodecs contains implements
var CmdHeaderCodecs = map[SerializeType]CmdHeaderCodec{
	SerialTypeJson:     new(jsonCodec),
	SerialTypeRocketMQ: new(rocketMQCodec),
}

//CmdHeaderCodec is the interface that wraps encode and decode method to Command
type CmdHeaderCodec interface {
	Encode(cmd *Command) ([]byte, error)
	Decode([]byte) (*Command, error)
}

type jsonCodec struct {
}

//Encode the command using json protocol
func (c *jsonCodec) Encode(cmd *Command) ([]byte, error) {
	return json.Marshal(cmd)
}

//Decode the command using json protocol
func (c *jsonCodec) Decode(b []byte) (cmd *Command, err error) {
	cmd = new(Command)
	err = json.Unmarshal(b, cmd)
	return
}

type rocketMQCodec struct {
}

//Encode the command using rocketmq protocol
func (c *rocketMQCodec) Encode(cmd *Command) ([]byte, error) {
	return nil, nil
}

//Decode the bytes using rocketmq protocol
func (c *rocketMQCodec) Decode(b []byte) (*Command, error) {
	return nil, nil
}

//ProducerData represents the producer group
type ProducerData struct {
	groupName string
}

//ConsumerType represents the consumer type
type ConsumerType struct {
	typeCN string
}

//MessageModel represents message model
type MessageModel struct {
	modeCN string
}

//ConsumeFromWhere specifies the offset type
type ConsumeFromWhere string

//Enum of consume offset
const (
	ConsumeFromLastOffset  ConsumeFromWhere = "CONSUME_FROM_LAST_OFFSET"
	ConsumeFromFirstOffset ConsumeFromWhere = "CONSUME_FROM_FIRST_OFFSET"
	ConsumeFromTimestamp   ConsumeFromWhere = "CONSUME_FROM_TIMESTAMP"
)

//ExpressionType is filter expression type
type ExpressionType string

//Enum of filter expression
const (
	SQL92 ExpressionType = "SQL92"
	TAG   ExpressionType = "TAG"
)

//SubscriptionData represents the subscription config
type SubscriptionData struct {
	classFilterMode bool
	topic           string
	subString       string
	tagsSet         []string
	codeSet         []int
	subVersion      int64
	expressionType  ExpressionType
}

//ConsumerData represent the consumer config
type ConsumerData struct {
	groupName           string
	consumeType         ConsumerType
	messageModel        MessageModel
	consumeFromWhere    ConsumeFromWhere
	subscriptionDataSet []SubscriptionData
	unitMode            bool
}

//HeartbeatData represents the data part of heartbeat req
type HeartbeatData struct {
	clientID        string
	producerDataSet []ProducerData
	consumerDataSet []ConsumerData
}

//Permission is the type of queue permission
type Permission int

//Enum of queue permission
const (
	PermPriority Permission = 1 << 3
	PermRead     Permission = 1 << 2
	PermWrite    Permission = 1 << 1
	PermInherit  Permission = 1 << 0
)

//PermitRead tests if has PermitRead flag
func PermitRead(perm Permission) bool {
	return (perm & PermRead) == PermRead
}

//PermitWrite tests if has PermWrite flag
func PermitWrite(perm Permission) bool {
	return (perm & PermWrite) == PermWrite
}

//PermitInherit tests if has PermInherit flag
func PermitInherit(perm Permission) bool {
	return (perm & PermInherit) == PermInherit
}
