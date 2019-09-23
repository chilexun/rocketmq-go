package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	mqclient "github.com/chilexun/rocketmq-go"
)

func sendMessage(producer mqclient.Producer) {
	msg := mqclient.Message{
		Topic: "TopicGoTest",
		Body:  []byte("Hello, go client!"),
	}
	result, err := producer.Send(msg, time.Second)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(&result)
	}
}

func main() {
	config := mqclient.NewProducerConfig()
	config.NamesrvAddr = []string{"192.168.199.171:9876"}
	producer, err := mqclient.NewProducer("TopicGoTest", config)
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
