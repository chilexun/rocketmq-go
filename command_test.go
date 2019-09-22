package mqclient

import (
	"reflect"
	"testing"
	"time"
)

func TestCommandCodec(t *testing.T) {
	props := map[string]string{MessageBuyID: "0001"}
	body := "Hello Word"
	request := &SendMessageRequest{
		ProducerGroup: "PID_Test_Producer_Group",
		Topic:         "test",
		QueueID:       1,
		SysFlag:       CompressedFlag,
		BornTimestamp: time.Now().UnixNano() / 1e6,
		Properties:    props,
		Body:          []byte(body),
	}
	cmd := SendMessage(request)
	encoded, err := EncodeCommand(&cmd, SerialTypeJSON)
	if err != nil {
		t.Fatal(err)
	}
	decodedCmd, err := DecodeCommand(encoded[4:])
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(cmd, *decodedCmd) {
		t.Log("PASS")
		return
	}
	t.Fail()
}
