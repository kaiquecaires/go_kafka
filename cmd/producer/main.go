package main

import (
	"fmt"
	"log"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	producer := NewKafkaProducer()
	deliveryChan := make(chan kafka.Event)
	Publish("Hello world", "teste", producer, []byte("transfer"), deliveryChan)
	go DeliveryReport(deliveryChan)
	producer.Flush(5000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "go_kafka-kafka-1:9092",
		"delivery.timeout.ms": "0",
		"acks":                "1",     // 0 -> dont need return, 1 wait for the leader, all to wait all brokers
		"enable.idempotence":  "false", // if this is true, o acks must be all
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	m := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key, // ensure same partition
	}

	err := producer.Produce(m, deliveryChan)
	return err
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("erro ao enviar")
			} else {
				// save on db
				fmt.Println("Mensagem enviada", ev.TopicPartition)
			}
		}
	}
}
