package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	Consumer *kafka.Consumer
	Storage  Storer
}

func NewConsumer(storage Storer) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9093",
		"broker.address.family":    "v4",
		"group.id":                 "group1",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		Consumer: c,
		Storage:  NewStorage(),
	}, nil
}

func (c *Consumer) consume(ctx context.Context) {
free:
	for {
		select {
		case <-ctx.Done():
			break free
		default:
			ev := c.Consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				_, err := c.Consumer.StoreMessage(e)
				if err != nil {
					fmt.Println("store msg err: ", err)
				}
				var msg Message
				if err := json.Unmarshal(e.Value, &msg); err != nil {
					log.Fatal(err)
				}
				if err := c.Storage.Put(msg.State, e.Value); err != nil {
					log.Fatal(err)
				}
			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					break
				}
			}
		}
	}
}

func (c *Consumer) Start(ctx context.Context) {
	c.consume(ctx)
}
