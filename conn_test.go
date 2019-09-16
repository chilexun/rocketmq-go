package mqclient

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"testing"
	"time"
)

var request Command
var response Command
var listener ConnEventListener
var wg sync.WaitGroup

type mockListener struct{}

func (*mockListener) OnMessage(cmd *Command) {
	fmt.Println("Response recieved:")
	fmt.Printf("Code: %d\n", cmd.Code)
	fmt.Printf("Opaque: %d\n", cmd.Opaque)
	fmt.Printf("Remark: %s\n", cmd.Remark)
	fmt.Printf("Remark: %v\n", cmd.ExtFields)
	wg.Done()
}

func (*mockListener) OnError(opaque int32, err error) {
	fmt.Printf("Request process Error, opaque:%d, %s", opaque, err)
}

func (*mockListener) OnIOError(c *Conn, err error) {
	fmt.Printf("IO Error, conn: %s", c.GetAddr())
}

func (*mockListener) OnClosed(c *Conn) {
	fmt.Printf("Connection closed: %s", c.GetAddr())
}

func init() {
	listener = &mockListener{}
	props := map[string]string{MessageBuyID: "0001"}
	body := "Hello Word"
	req := &SendMessageRequest{
		ProducerGroup: "PID_Test_Producer_Group",
		Topic:         "test",
		QueueID:       1,
		SysFlag:       CompressedFlag,
		BornTimestamp: time.Now().UnixNano() / 1e6,
		Properties:    props,
		Body:          []byte(body),
	}
	request = SendMessage(req)
	respExt := map[string]string{"msgId": "1001", "queueId": "1", "queueOffset": "1"}
	response = Command{Opaque: 1, Code: int(Success), ExtFields: respExt, Remark: "Hello, Welcome"}
	wg.Add(1)
	go startTCPServer()
}

func startTCPServer() {
	server, err := net.Listen("tcp", ":3000")
	if err != nil {
		fmt.Printf("Fail to start server:%s", err)
		return
	}
	defer server.Close()
	for {
		conn, err := server.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Printf("%s connected", conn.RemoteAddr().String())

		buf, err := ioutil.ReadAll(conn)
		if err != nil {
			fmt.Printf("Fail to start server:%s", err)
			return
		}
		fmt.Printf("Message received:%d \n", len(buf))

		respBuf, err := EncodeCommand(&response, SerialTypeJson)
		conn.Write(respBuf)
		return
	}
}

func TestWriteAndReadCmd(t *testing.T) {
	config := NewProducerConfig()
	conn := NewConn("127.0.0.1:3000", &config.ClientConfig, listener)
	time.Sleep(10)
	err := conn.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = conn.WriteCommand(ctx, &request)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	cancel()
}
