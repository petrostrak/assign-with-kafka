package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"math/rand"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

func produce(cancel context.CancelFunc) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	slog.Info("start producing", "topic", topic, "messages", lenMessages)
	for i := 0; i < lenMessages; i++ {
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
	cancel()
	slog.Info("start producing", "topic", topic, "messages", lenMessages)
}
