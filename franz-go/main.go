package main

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	seeds := []string{"127.0.0.1:46311"}
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics("kafka-health"),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	req := kmsg.NewMetadataRequest()
	ctx := context.Background()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		panic(err)
	}

	fmt.Println("List of topics:")
	for _, topic := range resp.Topics {
		fmt.Println(*topic.Topic)
	}

	cl.DiscoveredBrokers()

	record := &kgo.Record{Topic: "kafka-health", Value: []byte("bar")}

	// Alternatively, ProduceSync exists to synchronously produce a batch of records.
	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
	}
	fmt.Println("HI")
	// 2.) Consuming messages from a topic

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// All errors are retried internally when fetching, but non-retriable errors are
			// returned from polls so that users can notice and take action.
			panic(fmt.Sprint(errs))
		}

		// We can iterate through a record iterator...
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			fmt.Println(string(record.Value), "from an iterator!")
		}

		// or a callback function.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, record := range p.Records {
				fmt.Println(string(record.Value), "from range inside a callback!")
			}

			// We can even use a second callback!
			p.EachRecord(func(record *kgo.Record) {
				fmt.Println(string(record.Value), "from a second callback!")
			})
		})
	}

}
