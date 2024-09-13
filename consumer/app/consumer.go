package app

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type ConsumerGroup struct {
	sarama.ConsumerGroup
	Config *Config
}

func randomString(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	rand.Seed(time.Now().UnixNano())

	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func validateConfig(config *Config) error {
	if config.Brokers == nil {
		return errors.New("brokers not set")
	}
	if config.Topic == "" {
		return errors.New("topics not set")
	}

	if config.ConsumerGroup == "" {
		config.ConsumerGroup = "appscode-group" + randomString(4)
	}

	if config.Assignor == "" {
		config.Assignor = "range"
	}

	if config.SecurityProtocol == "" {
		config.SecurityProtocol = "PLAINTEXT"
	}

	return nil
}

func NewConsumer() (*ConsumerGroup, error) {
	config := NewConfig()
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	saramaConfig := config.GetConfig()
	cg, err := sarama.NewConsumerGroup(config.Brokers, config.ConsumerGroup, saramaConfig)
	if err != nil {
		return nil, err
	}
	return &ConsumerGroup{cg, config}, nil
}

func (c *ConsumerGroup) ConsumeMessages() {
	keepRunning := true
	cg, err := NewConsumer()
	if err != nil {
		log.Panicf("Error creating consumer group: %v", err)
	}

	consumer := Consumer{
		ready: make(chan bool),
	}
	ctx, cancel := context.WithCancel(context.Background())
	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := cg.Consume(ctx, strings.Split(c.Config.Topic, ","), &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			cg.toggleConsumptionFlow(&consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err = cg.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func (c *ConsumerGroup) toggleConsumptionFlow(isPaused *bool) {
	if *isPaused {
		c.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		c.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message claimed: key = %s, value = %s, timestamp = %v, topic = %s", string(message.Key), string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
