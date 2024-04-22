package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	produce()
	consumer, err := NewConsumer(NewStorage())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(time.Second * 3)
		consumer.Stop()
	}()

	consumer.Start()
	fmt.Printf("%+v\n", consumer.Storage.(*Storage).data)
}
