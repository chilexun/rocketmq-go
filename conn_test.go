package mqclient

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
	fmt.Printf("Request process Error, opaque:%d, %s\n", opaque, err)
}

func (*mockListener) OnIOError(c *Conn, err error) {
	fmt.Printf("IO Error, conn: %s\n", c.GetAddr())
}

func (*mockListener) OnClosed(c *Conn) {
	fmt.Printf("Connection closed: %s\n", c.GetAddr())
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

	go startTCPServer()
}

func startTCPServer() {
	server, err := net.Listen("tcp", ":3000")
	if err != nil {
		fmt.Printf("Fail to start server:%s", err)
		return
	}
	defer server.Close()

	conn, err := server.Accept()
	if err != nil {
		return
	}
	fmt.Printf("%s connected\n", conn.RemoteAddr().String())

	reader := bufio.NewReader(conn)
	var msgSize int32
	err = binary.Read(reader, binary.BigEndian, &msgSize)
	if err != nil {
		fmt.Printf("Fail to read msg size:%s", err)
		return
	}
	buf := make([]byte, msgSize)
	n, err := io.ReadFull(reader, buf)
	fmt.Printf("Message received, length: %d bytes\n", n)

	respBuf, err := EncodeCommand(&response, SerialTypeJson)
	conn.Write(respBuf)
	fmt.Printf("Write response to client, length: %d\n", len(respBuf))
	conn.Close()
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

	wg.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = conn.WriteCommand(ctx, &request)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
