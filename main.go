package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	lenMessages = 1000
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	produce(cancel)

	consumer, err := NewConsumer(NewStorage())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()

	consumer.Start(ctx)
	fmt.Printf("%+v\n", consumer.Storage.(*Storage).data)
}
