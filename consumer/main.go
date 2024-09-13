package main

import (
	"fmt"
	"github.com/obaydullahmhs/kafka-client-go/consumer/app"
	"os"
)

func main() {
	fmt.Println("Starting...")
	consumer, err := app.NewConsumer()
	fmt.Println(err)
	if err != nil {
		os.Exit(1)
	}
	consumer.ConsumeMessages()
}
