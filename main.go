package main

import (
	"fmt"
	"log"
)

func main() {
	produce()
	consumer, err := NewConsumer(NewStorage())
	if err != nil {
		log.Fatal(err)
	}
	consumer.Start()
	fmt.Printf("%+v\n", consumer.Storage.(*Storage).data)
}
