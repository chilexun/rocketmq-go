package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"time"

	mqclient "github.com/chilexun/rocketmq-go"
)

func sendMessage(producer mqclient.Producer) {
	msg := mqclient.NewMessage("TopicGoTest", []byte("Hello, go client!"))
	result, err := producer.Send(msg, time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(&result)
	}

	//Compress Body
	dataFile, err := os.Open("raw_message.txt")
	if os.IsNotExist(err) {
		fmt.Println("File raw_message.txt not exist")
		return
	}
	defer dataFile.Close()

	body, err := ioutil.ReadAll(dataFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	msg = mqclient.NewMessage("TopicGoTest", body)
	result, err = producer.Send(msg, time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(&result)
	}
}

func main() {
	config := mqclient.NewProducerConfig()
	config.NamesrvAddr = []string{"10.128.105.104:9876"}
	config.CompressMsgBodyOverHowmuch = 512
	producer, err := mqclient.NewProducer("PID_GO_TEST", config)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = producer.Start()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	go sendMessage(producer)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	s := <-ch
	fmt.Printf("Received exit signal, %v\n", s)
	producer.Shutdown()
}
