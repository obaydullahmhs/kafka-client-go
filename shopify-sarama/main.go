package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"time"
)

func main() {
	HI()
}
func HI() {
	brokerAddress := []string{"localhost:9092"}
	//brokerAddress := []string{"kafka-quickstart-0.kafka-quickstart-pods.demo.svc.cluster.local:9092", "kafka-quickstart-1.kafka-quickstart-pods.demo.svc.cluster.local:9092",
	//	"kafka-quickstart-2.kafka-quickstart-pods.demo.svc.cluster.local:9092"}
	// create a broker client
	//broker := sarama.NewBroker(brokerAddress[0])
	config := sarama.NewConfig()
	//err := broker.Open(config)
	//defer func(broker *sarama.Broker) {
	//	err := broker.Close()
	//	if err != nil {
	//		return
	//	}
	//}(broker)
	//
	//if err != nil {
	//	fmt.Println("failed to open broker connection")
	//	return
	//}
	//fmt.Println(broker.Connected())
	// create a cluster admin
	admin, err := sarama.NewClusterAdmin(brokerAddress, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	if err != nil {
		fmt.Println("Failed to create cluster admin:", err)
		return
	}
	defer func(admin sarama.ClusterAdmin) {
		err := admin.Close()
		if err != nil {
			return
		}
	}(admin)
	client, err := sarama.NewClient(brokerAddress, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func(client sarama.Client) {
		err := client.Close()
		if err != nil {

		}
	}(client)
	_, err = client.RefreshController()
	if err != nil {
		return
	}
	err = client.RefreshMetadata()
	if err != nil {
		return
	}

	//createTopic("my-topic", admin, 1, 1)
	//printTopics(admin)
	//
	//for {
	//	ProducerM(config, brokerAddress)
	//}
	go consumeMessage(brokerAddress, config)
	//for i := 0; i < 10; i++ {
	//	fmt.Println("waiting top")
	//	time.Sleep(10 * time.Second)
	//}
	//ch := make(chan int)
	//go func() {
	//	for i := 0; i < 100; i++ {
	//		ch <- 1
	//		fmt.Println("waiting top")
	//	}
	//}()
	//go func() {
	//	for i := 0; i < 100; i++ {
	//		fmt.Println(<-ch)
	//		time.Sleep(10 * time.Second)
	//	}
	//}()
	time.Sleep(1000 * time.Second)

	//deleteTopic("my-topic", admin)
	//time.Sleep(5 * time.Second)
	//describeCluster(admin)

	//SeeConfig(client)
	//_, err = fmt.Scanln()
	//if err != nil {
	//	return
	//}
}

func ProducerM(config *sarama.Config, brokerAddress []string) {
	config.Producer.Return.Successes = true
	// Create a new producer
	producer, err := sarama.NewSyncProducer(brokerAddress, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %s", err)
		}
	}()
	msg := "Hi, KubeDB" + time.Now().String()
	sendMessage("my-topic", msg, producer)
	fmt.Println(msg)
	time.Sleep(10 * time.Second)
}

func sendMessage(topic string, messageValue string, producer sarama.SyncProducer) {
	// Create a new message
	message := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(messageValue),
		Partition: 0,
	}

	// Send the message
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %s", err)
	}

	fmt.Printf("Message sent successfully! Partition: %d, Offset: %d\n", partition, offset)
}

func consumeMessage(brokerAddress []string, config *sarama.Config) {
	// Create a new consumer
	consumer, err := sarama.NewConsumer(brokerAddress, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close consumer: %s", err)
		}
	}()
	topic := "my-topic"
	partition := int32(0)
	// Set up a partition consumer for the topic
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, -2)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %s", err)
	}
	fmt.Println(partitionConsumer)
	//defer func() {
	//	if err := partitionConsumer.AsyncClose(); err != nil {
	//		log.Printf("Failed to close partition consumer: %s", err)
	//	}
	//}()
	// defer partitionConsumer.AsyncClose()
	var messages []string
	//select {
	//case msg := <-partitionConsumer.Messages():
	//	messages = append(messages, string(msg.Value))
	//	fmt.Println(string(msg.Value))
	//case err := <-partitionConsumer.Errors():
	//	fmt.Printf("Error while consuming message: %v", err)
	//	break
	//}
	//channel := make(chan int)
	//wg := sync.WaitGroup{}
	//wg.Add(1)
	go func(pc sarama.PartitionConsumer) {
		//defer wg.Done()
		defer pc.AsyncClose()
		for {
			select {
			//case <-channel:
			//	fmt.Println("Done")
			//	break
			case msg, ok := <-pc.Messages():
				fmt.Println(ok)
				if !ok {
					return
				}
				messages = append(messages, string(msg.Value))
				fmt.Println(string(msg.Value))
			case err := <-pc.Errors():
				fmt.Printf("Error while consuming message: %v", err)
				break
			}
		}
		//for message := range partitionConsumer.Messages() {
		//	messages = append(messages, string(message.Value))
		//	fmt.Println(string(message.Value))
		//}
	}(partitionConsumer)

	time.Sleep(10 * time.Second)
	partitionConsumer.AsyncClose()
	//for {
	//	fmt.Println("kk")
	//	time.Sleep(10 * time.Second)
	//	//break
	//}
	//wg.Wait()
	//fmt.Println(messages)

	//msg := <-partitionConsumer.Messages()
	//fmt.Println("consumer:", string(msg.Value))

	// Start consuming messages
	//for message := range partitionConsumer.Messages() {
	//	fmt.Printf("Received message. Partition: %d, Offset: %d, Value: %s\n", message.Partition, message.Offset, string(message.Value))
	//}

	//wg.Add(1)
	//go SeeMessage(partitionConsumer)
	//wg.Add(1)
	//go func(pc sarama.PartitionConsumer) {
	//	defer wg.Done()
	//	for {
	//		select {
	//		case msg := <-pc.Messages():
	//			fmt.Println(string(msg.Value), msg.Offset)
	//		case err := <-pc.Errors():
	//			fmt.Println("Error while consuming message: %v", err)
	//		}
	//	}
	//for message := range pc.Messages() {
	//	fmt.Println(message)
	//}
	//}(partitionConsumer)

	//wg.Wait()

	//go func(pc sarama.PartitionConsumer) {
	//	received := false
	//	for !received {
	//		select {
	//		case message := <-pc.Messages():
	//			//fmt.Println(fmt.Sprintf("%v-- \n%v", msg.val, string(message.Value)))
	//			fmt.Println(message)
	//		case err := <-pc.Errors():
	//			fmt.Println(fmt.Sprintf("could not process message, err: %v", err))
	//			received = true
	//			break
	//		}
	//		//if msg.val == string(message.Value) {
	//		//	r.Log.Info(fmt.Sprintf("received message %v\n", string(message.Value)))
	//		//	received = true
	//		//}
	//	}
	//}(partitionConsumer)
	//time.Sleep(10 * time.Second)
	time.Sleep(5 * time.Second)
	defer fmt.Println("HI I am done")

}
func SeeConfig(client sarama.Client) {
	fmt.Println(client.Brokers()[0])
	cn := client.Config()
	fmt.Println(cn)
}

func SeeMessage(pc sarama.PartitionConsumer) {
	//defer wg.Done()
	var messages []string
	//select {
	//case msg := <-pc.Messages():
	//	fmt.Println("kjdf", msg == nil)
	//	//messages = append(messages, string(msg.Value))
	//case err := <-pc.Errors():
	//	fmt.Println("Error while consuming message: %v", err)
	//}
	for message := range pc.Messages() {
		fmt.Println(message.Value)
	}
	fmt.Println(len(messages), messages)
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
	dd, err := admin.DescribeLogDirs([]int32{0, 1, 2, 3})
	if err != nil {
		return
	}
	fmt.Println(dd)
	x, err := admin.Controller()
	fmt.Println(x)

	//admin.AlterConfig()

	// Describe configurations for the specified broker.
	brokerConfigs, err := admin.DescribeConfig(sarama.ConfigResource{Type: sarama.BrokerResource, Name: fmt.Sprintf("%d", int32(5))})
	if err != nil {
		fmt.Println("Error describing broker configurations:", err)
		os.Exit(1)
	}

	fmt.Printf("Broker Configurations for Broker ID %d:\n", int32(5))
	for _, configEntry := range brokerConfigs {
		if configEntry.Name == "controller.quorum.voters" {
			break
		}
		fmt.Printf("Config Key: %s, Config Value: %s\n", configEntry.Name, configEntry.Value)
	}

	z, err := admin.DescribeTopics([]string{"test"})
	fmt.Println(z[0].Partitions[0].Replicas)

	//admin.AlterPartitionReassignments("test")
	y, err := admin.ListPartitionReassignments("test", []int32{0})

	fmt.Println("HI", y)

	partitionReassignment := make([][]int32, 5)
	fmt.Println(len(partitionReassignment), len(partitionReassignment[0]))
}

//func topicReassignment(admin sarama.ClusterAdmin) error {
//	admin.AlterPartitionReassignments()
//
//	return nil
//}
