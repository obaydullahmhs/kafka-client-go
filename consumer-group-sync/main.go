package main

import (
	"fmt"
	"github.com/obaydullahmhs/kafka-client-go/consumer-group-sync/app"
	"os"
)

func main() {
	fmt.Println("Starting...")
	cg, err := app.NewClient()
	fmt.Println(err)
	if err != nil {
		os.Exit(1)
	}
	err = cg.SyncSourceConsumerGroup()
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	fmt.Println("Done!")
}
