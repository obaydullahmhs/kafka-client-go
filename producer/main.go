package main

import (
	"fmt"
	"github.com/obaydullahmhs/kafka-client-go/producer/app"
	"os"
)

func main() {
	fmt.Println("Starting...")
	producer, err := app.NewProducer()
	fmt.Println(err)
	if err != nil {
		os.Exit(1)
	}
	producer.SendRandomMessages()
	fmt.Println("Done!")
}
