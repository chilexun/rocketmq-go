package mqclient

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"sync/atomic"
)

var requestID int32

//Command represents a rpc cmd between servers
type Command struct {
	Code      int               `json:"code"`
	Opaque    int32             `json:"opaque"`
	Version   int               `json:"version"`
	Body      []byte            `json:"-"`
	Remark    string            `json:"remark,omitempty"`
	ExtFields map[string]string `json:"extFields,omitempty"`
}

//SendMessageRequest represents a message send rpc request
type SendMessageRequest struct {
	ProducerGroup     string
	Topic             string
	QueueID           int
	SysFlag           MsgSysFlag
	BornTimestamp     int64
	Flag              int
	Properties        map[string]string
	ReconsumeTimes    int
	UnitMode          bool
	Batch             bool
	MaxReconsumeTimes int
	Body              []byte
}

func (r *SendMessageRequest) toExtFields() map[string]string {
	fields := make(map[string]string, 11)
	fields["producerGroup"] = r.ProducerGroup
	fields["topic"] = r.Topic
	fields["queueId"] = strconv.Itoa(r.QueueID)
	fields["sysFlag"] = strconv.Itoa(int(r.SysFlag))
	fields["bornTimestamp"] = strconv.FormatInt(r.BornTimestamp, 64)
	fields["flag"] = strconv.Itoa(r.Flag)
	fields["properties"] = msgProps2String(r.Properties)
	fields["reconsumeTimes"] = strconv.Itoa(r.ReconsumeTimes)
	fields["unitMode"] = strconv.FormatBool(r.UnitMode)
	fields["batch"] = strconv.FormatBool(r.Batch)
	fields["maxReconsumeTimes"] = strconv.Itoa(r.MaxReconsumeTimes)
	return fields
}

//SendMessageResponse wraps a message send command response
type SendMessageResponse struct {
	Code          ResponseCode
	MsgID         string
	QueueID       int
	QueueOffset   int64
	TransactionID string
	Remark        string
}

func (r *SendMessageResponse) fromExtFields(fields map[string]string) (err error) {
	r.MsgID = fields["msgId"]
	r.QueueID, err = strconv.Atoi(fields["queueId"])
	r.QueueOffset, err = strconv.ParseInt(fields["queueOffset"], 10, 64)
	r.TransactionID = fields["transactionId"]
	return
}

func getRequestID() int32 {
	return atomic.AddInt32(&requestID, 1)
}

//GetRouteInfo create a GetRouteInfoByTopic command
func GetRouteInfo(topic string) Command {
	header := make(map[string]string, 1)
	header["topic"] = topic
	return Command{Opaque: getRequestID(), Code: int(GetRouteInfoByTopicReq), ExtFields: header}
}

//SendMessage create a SendMessage command
func SendMessage(request *SendMessageRequest) Command {
	return Command{Opaque: getRequestID(), Code: int(SendMessageReq), Body: request.Body, ExtFields: request.toExtFields()}
}

//HeartBeat create a HeartBeat command
func HeartBeat(data HeartbeatData) Command {
	body, _ := json.Marshal(data)
	return Command{Opaque: getRequestID(), Code: int(HeartBeatReq), Body: body}
}

//EncodeCommand encode the command with the specify serial type
func EncodeCommand(c *Command, serialType SerializeType) ([]byte, error) {
	var length = 4
	headerData, err := CmdHeaderCodecs[serialType].Encode(c)
	if err != nil {
		return nil, err
	}

	length += len(headerData)
	if c.Body != nil {
		length += len(c.Body)
	}
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(length))
	buffer.Write(markProtocolType(len(headerData), serialType))
	buffer.Write(headerData)
	if c.Body != nil {
		buffer.Write(c.Body)
	}
	return buffer.Bytes(), nil
}

//DecodeCommand decode the byte array from server
func DecodeCommand(data []byte) (cmd *Command, err error) {
	buffer := bytes.NewBuffer(data)
	var length int32
	err = binary.Read(buffer, binary.BigEndian, &length)
	if err != nil {
		return
	}

	var mark int32
	err = binary.Read(buffer, binary.BigEndian, &mark)
	if err != nil {
		return nil, err
	}
	serialType := SerializeType((mark >> 24) & 0xFF)
	headerLength := mark & 0xFFFFFF
	headerData := make([]byte, headerLength)
	_, err = buffer.Read(headerData)
	if err != nil {
		return nil, err
	}
	cmd, err = CmdHeaderCodecs[serialType].Decode(headerData)
	if err != nil {
		return
	}

	bodyLength := length - 4 - headerLength
	if bodyLength > 0 {
		body := make([]byte, bodyLength)
		_, err = buffer.Read(body)
		cmd.Body = body
	}
	return
}

func markProtocolType(source int, serialType SerializeType) []byte {
	result := make([]byte, 4)
	result[0] = byte(serialType)
	result[1] = (byte)((source >> 16) & 0xFF)
	result[2] = (byte)((source >> 8) & 0xFF)
	result[3] = (byte)(source & 0xFF)
	return result
}

func msgProps2String(props map[string]string) string {
	var builder strings.Builder
	for k, v := range props {
		builder.WriteString(k)
		builder.WriteByte(MessageNameValueSeparator)
		builder.WriteString(v)
		builder.WriteByte(MessagePropertySeparator)
	}
	return builder.String()
}
