package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go_kafka-kafka-1:9092",
		"client.id":         "goapp-consumer", // change for more workers
		"group.id":          "goapp-group",    // change for new group
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		fmt.Println("error consumer", err.Error())
		return
	}

	topics := []string{"teste"}
	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}
