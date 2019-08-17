package mqclient

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
)

type RequestHeader interface {
	responseType() reflect.Type
}

type Command struct {
	Code         RequestCode       `json:"code"`
	Opaque       int32             `json:"opaque"`
	CustomHeader interface{}       `json:"-"`
	Version      int               `json:"version"`
	Body         []byte            `json:"-"`
	Remark       string            `json:"remark,omitempty"`
	ExtFields    map[string]string `json:"extFields,omitempty"`
}

type SendMessageRequestHeader struct {
	ProducerGroup     string
	Topic             string
	QueueId           int
	SysFlag           MsgSysFlag
	BornTimestamp     int64
	Flag              int
	Properties        string
	ReconsumeTimes    int
	UnitMode          bool
	Batch             bool
	MaxReconsumeTimes int
}

type SendMessageResponseHeader struct {
	MsgId         string
	QueueId       int
	QueueOffset   int64
	TransactionId string
}

func GetRouteInfo(topic string) Command {
	header := make(map[string]string, 1)
	header["topic"] = topic
	return Command{Code: GET_ROUTEINTO_BY_TOPIC, ExtFields: header}
}

func SendMessage(header *SendMessageRequestHeader, body []byte) Command {
	return Command{Code: SEND_MESSAGE, CustomHeader: header, Body: body}
}

func HeartBeat(data HeartbeatData) Command {
	body, _ := json.Marshal(data)
	return Command{Code: HEART_BEAT, Body: body}
}

func (c *Command) Encode() ([]byte, error) {
	var length int = 4
	headerData, _ := c.encodeHeader()
	length += len(headerData)
	if c.Body != nil {
		length += len(c.Body)
	}
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(length))
	buffer.Write(markProtocolType(len(headerData), JSON))
	buffer.Write(headerData)
	if c.Body != nil {
		buffer.Write(c.Body)
	}
	return buffer.Bytes(), nil
}

func Decode(data []byte) (*Command, error) {
	buffer := bytes.NewBuffer(data)
	var length int32
	err := binary.Read(buffer, binary.BigEndian, &length)
	if err != nil {
		return nil, err
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
	cmd := DecodeHeader(headerData, serialType)

	bodyLength := length - 4 - headerLength
	if bodyLength > 0 {
		body := make([]byte, bodyLength)
		buffer.Read(body)
		cmd.Body = body
	}

	return cmd, nil
}

func (c *Command) ResolveCustomHeader(t reflect.Type) {
	header := reflect.New(t.Elem()).Elem()
	for k, v := range c.ExtFields {
		if f := header.FieldByName(k); f.CanSet() {
			setFieldValue(f, v)
		}
	}
}

func setFieldValue(field reflect.Value, v string) {
	switch field.Type().String() {
	case "string":
		field.SetString(v)
	case "int", "int16", "int32", "int64":
		if i, err := strconv.Atoi(v); err == nil {
			field.SetInt(int64(i))
		}
	case "uint", "uint16", "uint32", "uint64":
		if i, err := strconv.Atoi(v); err == nil {
			field.SetUint(uint64(i))
		}
	case "float32":
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			field.SetFloat(float64(f))
		}
	case "float64":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			field.SetFloat(float64(f))
		}
	case "bool":
		if b, err := strconv.ParseBool(v); err == nil {
			field.SetBool(b)
		}
	}
}

func (c *Command) encodeHeader() ([]byte, error) {
	if c.CustomHeader != nil {
		elemT := reflect.TypeOf(c.CustomHeader).Elem()
		elemV := reflect.ValueOf(c.CustomHeader).Elem()
		for i := 0; i < elemT.NumField(); i++ {
			f := elemT.Field(i)
			c.ExtFields[lowerFirstCh(f.Name)] = CoerceString(elemV.FieldByName(f.Name).InterfaceData())
		}
	}
	return json.Marshal(c)
}

func lowerFirstCh(str string) string {
	return strings.ToLower(str[:1]) + str[1:]
}

func upperFirstCh(str string) string {
	return strings.ToUpper(str[:1]) + str[1:]
}

func DecodeHeader(data []byte, serialType SerializeType) *Command {
	cmd := new(Command)
	json.Unmarshal([]byte{}, cmd)
	return cmd
}

func markProtocolType(source int, serialType SerializeType) []byte {
	result := make([]byte, 4)
	result[0] = byte(serialType)
	result[1] = (byte)((source >> 16) & 0xFF)
	result[2] = (byte)((source >> 8) & 0xFF)
	result[3] = (byte)(source & 0xFF)
	return result
}
