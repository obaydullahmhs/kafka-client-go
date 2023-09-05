package main

import (
	"C"
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
)

func main() {
	// Specify the Kafka broker(s) address
	broker := "127.0.0.1:9095"

	// Create AdminClient configuration
	adminConfig := &kafka.ConfigMap{"bootstrap.servers": broker}

	// Create AdminClient instance
	admin, err := kafka.NewAdminClient(adminConfig)
	if err != nil {
		fmt.Printf("Failed to create AdminClient: %v\n", err)
		return
	}

	// Specify the topic name and configuration
	//createTopic(admin, "my-topic")
	//deleteTopic(admin, "my-topic")
	printTopics(broker)
	//produceMessage()
	//	consumeMessage()
	// Close AdminClient
	err = Ping(admin)
	if err != nil {
		return
	}
	admin.Close()
}
func Ping(admin *kafka.AdminClient) error {
	topic := "sample"
	meta, err := admin.GetMetadata(&topic, false, 1000)
	if err != nil {
		return err
	}
	fmt.Println(meta)
	return nil
}
func consumeMessage(topics ...string) {
	broker := "127.0.0.1:9092"
	topic := "my-topic"
	if len(topics) > 0 && topics[0] == "" {
		topic = topics[0]
	}

	// Consumer configuration
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "my-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	// Subscribe to topic
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		panic(err)
	}

	// Consume messages
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received message: %s\n", string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func produceMessage() {
	// Kafka broker configuration
	broker := "127.0.0.1:9092"
	topic := "my-topic"

	// Producer configuration
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		panic(err)
	}

	// Delivery report handler for producer
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("Message-%d", i)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
		}, nil)
		if err != nil {
			panic(err)
		}
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
func createTopic(admin *kafka.AdminClient, topic string) {
	topicConfig := &kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	// Create a list of topics to be created
	topics := []kafka.TopicSpecification{*topicConfig}

	// Create topics using AdminClient

	results, err := admin.CreateTopics(context.Background(), topics, kafka.SetAdminOperationTimeout(5000*time.Millisecond))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		return
	}

	// Check the results of topic creation
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Printf("Failed to create topic %s: %v\n", result.Topic, result.Error)
		} else {
			fmt.Printf("Topic %s created\n", result.Topic)
		}
	}
}

func printTopics(brokers string) error {
	//brokers := "127.0.0.1:9092"    // Replace with your Kafka broker addresses
	groupID := "my-consumer-group" // Replace with your consumer group ID

	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer func(consumer *kafka.Consumer) {
		err := consumer.Close()
		if err != nil {
			return
		}
	}(consumer)

	topics, err := consumer.GetMetadata(nil, true, 1000)
	if err != nil {
		return err
	}

	fmt.Println("Kafka topics:")
	for _, topic := range topics.Topics {
		fmt.Println(topic.Topic)
	}
	return nil
}

func deleteTopic(admin *kafka.AdminClient, topic string) {
	deleteResults, err := admin.DeleteTopics(context.Background(), []string{topic}, kafka.SetAdminOperationTimeout(5000*time.Millisecond))
	if err != nil {
		fmt.Printf("Failed to delete topic %s: %v\n", topic, err)
		return
	}

	for _, result := range deleteResults {
		if result.Error.Code() != kafka.ErrNoError {
			fmt.Printf("Failed to delete topic %s: %v\n", topic, result.Error)
		} else {
			fmt.Printf("Topic %s deleted successfully\n", topic)
		}
	}
}
