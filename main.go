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
	if err := produce(); err != nil {
		fmt.Println(err)
	}
}

func produce() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	var states = []int{0, 1, 2}
	for i := 0; i < 1000; i++ {
		msg := Message{
			State: MessageState(states[rand.Intn(len(states))]),
		}
		b, err := json.Marshal(msg)
		if err != nil {
			return err
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

	return nil
}
