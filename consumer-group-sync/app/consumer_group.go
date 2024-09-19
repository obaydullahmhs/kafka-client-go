package app

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"os/signal"
)

type ConsumerGroup struct {
	source       sarama.ClusterAdmin
	target       sarama.ClusterAdmin
	sourceConfig *Config
	targetConfig *Config
}

func validateConfig(config *Config) error {
	if config.Brokers == nil {
		return errors.New("brokers not set")
	}

	if config.SecurityProtocol == "" {
		config.SecurityProtocol = "PLAINTEXT"
	}

	return nil
}

func NewClient() (*ConsumerGroup, error) {
	sourceConfig := NewConfig("SOURCE")
	saramaSourceConfig := sourceConfig.GetConfig()
	if err := validateConfig(sourceConfig); err != nil {
		return nil, err
	}
	sourceAdminClient, err := sarama.NewClusterAdmin(sourceConfig.Brokers, saramaSourceConfig)
	if err != nil {
		return nil, err
	}
	targetConfig := NewConfig("TARGET")
	saramaTargetConfig := sourceConfig.GetConfig()
	if err := validateConfig(targetConfig); err != nil {
		return nil, err
	}
	targetAdminClient, err := sarama.NewClusterAdmin(targetConfig.Brokers, saramaTargetConfig)
	if err != nil {
		return nil, err
	}
	return &ConsumerGroup{
		source:       sourceAdminClient,
		target:       targetAdminClient,
		sourceConfig: sourceConfig,
		targetConfig: targetConfig,
	}, nil
}

func (c *ConsumerGroup) SyncSourceConsumerGroup() error {
	cg, err := c.source.ListConsumerGroups()
	if err != nil {
		return err
	}
	for key, _ := range cg {
		err = c.CloneConsumerGroup(key, key)
		if err != nil {
			return err
		}
	}
	return nil
}

type PartitionOffset struct {
	Offset   int64
	Metadata string
}

func (c *ConsumerGroup) CloneConsumerGroup(srcGroup, targetGroup string) error {
	fmt.Println("Syncing consumer group", srcGroup, "to", targetGroup)
	var (
		err        error
		srcOffsets *sarama.OffsetFetchResponse
	)
	if srcOffsets, err = c.source.ListConsumerGroupOffsets(srcGroup, nil); err != nil {
		return errors.Wrapf(err, "failed to get consumerGroup '%s' offsets", srcGroup)
	}

	if len(srcOffsets.Blocks) == 0 {
		return errors.Errorf("consumerGroup '%s' does not contain offsets", srcGroup)
	}

	//if targetOffsets, err = c.target.ListConsumerGroupOffsets(targetGroup, nil); err != nil {
	//	return errors.Wrapf(err, "failed to get consumerGroup '%s' offsets", targetGroup)
	//}
	//

	topicPartitionOffsets := make(map[string]map[int32]PartitionOffset) // topic->partition->offset
	for topic, partitions := range srcOffsets.Blocks {
		p := topicPartitionOffsets[topic]
		if p == nil {
			p = make(map[int32]PartitionOffset)
		}

		for partition, block := range partitions {
			p[partition] = PartitionOffset{Offset: block.Offset, Metadata: block.Metadata}
		}

		topicPartitionOffsets[topic] = p
	}

	consumerGroup, err := sarama.NewConsumerGroup(c.targetConfig.Brokers, targetGroup, c.targetConfig.GetConfig())
	if err != nil {
		return errors.Errorf("failed to create consumer group %s: %v", targetGroup, err)
	}

	terminalCtx := CreateTerminalContext()

	consumeErrorGroup, _ := errgroup.WithContext(terminalCtx)
	consumeErrorGroup.SetLimit(100)

	for topic, partitionOffsets := range topicPartitionOffsets {
		topicName, offsets := topic, partitionOffsets
		consumeErrorGroup.Go(func() error {
			consumer := Consumer{
				Topic:            topicName,
				PartitionOffsets: offsets,
			}

			err = consumerGroup.Consume(terminalCtx, []string{topicName}, &consumer)
			if err != nil {
				return err
			}
			<-consumer.ready
			return nil
		})
	}

	err = consumeErrorGroup.Wait()
	if err != nil {
		return err
	}

	err = consumerGroup.Close()
	if err != nil {
		return err
	}
	fmt.Println("Consumer group sync completed")
	c.targetConfig.Log.Println("Consumer group sync completed")

	return nil
}

type Consumer struct {
	Topic            string
	PartitionOffsets map[int32]PartitionOffset
	ready            chan struct{}
}

func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	consumer.ready = make(chan struct{})

	for partition, offset := range consumer.PartitionOffsets {
		fmt.Println("Setting offset", offset.Offset, "for partition", partition)
		session.MarkOffset(consumer.Topic, partition, offset.Offset, offset.Metadata)
	}
	return nil
}

func (consumer *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return nil
}

func CreateTerminalContext() context.Context {
	ctx := context.Background()

	// trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(ctx)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		select {
		case <-signals:
			log.Println("terminating: via signal")
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx
}
