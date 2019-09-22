package main

import (
	"fmt"
	"os"
	"os/signal"

	mqclient "github.com/chilexun/rocketmq-go"
)

func sendMessage() {

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

	go sendMessage()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	s := <-ch
	fmt.Printf("Received exit signal, %v\n", s)
	producer.Shutdown()
}
