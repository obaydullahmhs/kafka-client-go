package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	ID        int32     `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type Producer struct {
	sarama.SyncProducer
	Config *Config
}

func validateConfig(config *Config) error {
	if config.Brokers == nil {
		return errors.New("brokers not set")
	}
	if config.Topic == "" {
		return errors.New("topics not set")
	}

	if config.SecurityProtocol == "" {
		config.SecurityProtocol = "PLAINTEXT"
	}

	return nil
}

func NewProducer() (*Producer, error) {
	config := NewConfig()
	saramaConfig := config.GetConfig()
	saramaConfig.Producer.Return.Successes = true
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}
	return &Producer{producer, config}, nil
}

func (p *Producer) SendRandomMessages() {
	i := 0
	for {
		if isPaused() {
			continue
		}
		if p.Config.MaxMessages == 0 {
			break
		}
		p.Config.MaxMessages--
		key := sarama.StringEncoder(fmt.Sprintf("Random-%d", i))
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		message := Message{
			ID:        int32(i),
			Content:   fmt.Sprintf("M-%d", r.Int31()),
			Timestamp: time.Now(),
		}
		value, err := json.MarshalIndent(message, "", "  ")
		if err != nil {
			p.Config.Log.Error("failed to marshal message", err)
		}
		msg := &sarama.ProducerMessage{
			Topic: p.Config.Topic,
			Key:   key,
			Value: sarama.ByteEncoder(value),
		}
		_, _, err = p.SendMessage(msg)
		if err != nil {
			p.Config.Log.Error("failed to send message", err)
		}
		i++

		if p.Config.SleepTimeMS != -1 {
			time.Sleep(time.Duration(p.Config.SleepTimeMS) * time.Millisecond)
		}
	}
}

func isPaused() bool {
	homeDir := os.Getenv("HOME")
	if homeDir == "" {
		fmt.Println("HOME environment variable is not set.")
		return false
	}

	// Construct the file path to $HOME/pause
	filePath := filepath.Join(homeDir, "pause")
	if _, err := os.Stat(filePath); err == nil {
		return true
	}

	return false
}
