package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	brokerAddress := []string{"localhost:9092"}
	// create a broker client
	broker := sarama.NewBroker(brokerAddress[0])
	config := sarama.NewConfig()
	err := broker.Open(config)
	defer broker.Close()
	if err != nil {
		fmt.Println("failed to open broker connection")
		return
	}
	fmt.Println(broker.Connected())
	// create a cluster admin
	admin, err := sarama.NewClusterAdmin(brokerAddress, config)
	defer admin.Close()
	if err != nil {
		fmt.Println("Failed to create cluster admin:", err)
		return
	}

	createTopic("my-topic", admin, 1, 1)
	//deleteTopic("my-topic", admin)
	describeCluster(admin)
	printTopics(admin)
	config.Producer.Return.Successes = true
	// Create a new producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %s", err)
		}
	}()
	sendMessage("my-topic", "Hi, KubeDB", producer)

	// Create a new consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close consumer: %s", err)
		}
	}()
	consumeMessage("my-topic", 0, consumer)
}
func consumeMessage(topic string, partition int32, consumer sarama.Consumer) {
	// Set up a partition consumer for the topic
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %s", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Printf("Failed to close partition consumer: %s", err)
		}
	}()

	// Start consuming messages
	for message := range partitionConsumer.Messages() {
		fmt.Printf("Received message. Partition: %d, Offset: %d, Value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}

func sendMessage(topic string, messageValue string, producer sarama.SyncProducer) {
	// Create a new message
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(messageValue),
	}

	// Send the message
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %s", err)
	}

	fmt.Printf("Message sent successfully! Partition: %d, Offset: %d\n", partition, offset)
}
func printTopics(admin sarama.ClusterAdmin) {
	topics, err := admin.ListTopics()
	if err != nil {
		fmt.Println("Failed to fetch topic list:", err)
		return
	}
	// Print the list of topics
	fmt.Println("Topics:")
	for topic := range topics {
		fmt.Println(topic)
	}
}

func createTopic(topic string, admin sarama.ClusterAdmin, partition int32, replication int16) {
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partition,   // Number of partitions for the topic
		ReplicationFactor: replication, // Replication factor for the topic
	}

	topicConfigs := make(map[string]*sarama.TopicDetail)
	topicConfigs[topic] = topicDetail
	err := admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		fmt.Println("Failed to create topic:", err)
		return
	}

	fmt.Println("Topic created successfully!")
	return
}

func deleteTopic(topic string, admin sarama.ClusterAdmin) {
	err := admin.DeleteTopic(topic)
	if err != nil {
		fmt.Println("Failed to delete topic:", err)
		return
	}
	fmt.Println("Topic deleted successfully")
}
func describeCluster(admin sarama.ClusterAdmin) {
	brokers, controllerID, err := admin.DescribeCluster()
	if err != nil {
		fmt.Println("Failed to get cluster metadata")
		return
	}

	// Iterate over the brokers
	for _, broker := range brokers {
		fmt.Printf("Broker ID: %d, Address: %s\n", broker.ID(), broker.Addr())
	}

	fmt.Printf("Controller Broker ID: %d\n", controllerID)
}
