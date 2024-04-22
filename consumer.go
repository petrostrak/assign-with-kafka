package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
)

type Consumer struct {
	Consumer *kafka.Consumer
	Storage  Storer
	Quit     chan any
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
		Quit:     make(chan any),
	}, nil
}

func (c *Consumer) Stop() {
	c.Quit <- struct{}{}
}

func (c *Consumer) consume() {
free:
	for {
		select {
		case <-c.Quit:
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

func (c *Consumer) Start() {
	c.consume()
}
