package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"math/rand"
)

var (
	topic = "assigntopic"
)

type Message struct {
	State MessageState
}

type MessageState int

const (
	MessageStateFailed MessageState = iota
	MessageStateCompleted
	MessageStateInProgress
)

func main() {
	produce()
	consume()
}

func produce() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 1000; i++ {
		msg := Message{
			State: MessageState(rand.Intn(3)),
		}
		b, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: b,
		}, nil)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func consume() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9093",
		"broker.address.family":    "v4",
		"group.id":                 "group1",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal(err)
	}

	for {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			_, err := c.StoreMessage(e)
			if err != nil {
				fmt.Println("store msg err: ", err)
			}
			var msg Message
			if err := json.Unmarshal(e.Value, &msg); err != nil {
				log.Fatal(err)
			}
			fmt.Println(msg)
		case kafka.Error:
			if e.Code() == kafka.ErrAllBrokersDown {
				break
			}
		}
	}
}
