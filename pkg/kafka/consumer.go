package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumerConfig defines settings for reading from Kafka.
type ConsumerConfig struct {
	Brokers       []string      `json:"brokers" yaml:"brokers"`
	GroupID       string        `json:"group_id" yaml:"group_id"`
	Topics        []string      `json:"topics" yaml:"topics"`
	MinBytes      int           `json:"min_bytes,omitempty" yaml:"min_bytes,omitempty"`
	MaxBytes      int           `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
	MaxWait       time.Duration `json:"max_wait,omitempty" yaml:"max_wait,omitempty"`
	QueueCapacity int           `json:"queue_capacity,omitempty" yaml:"queue_capacity,omitempty"`
	StartOffset   int64         `json:"start_offset,omitempty" yaml:"start_offset,omitempty"`
}

// Consumer wraps kafka-go.Reader to pull messages from one or more topics.
type Consumer struct {
	reader *kafka.Reader
	cfg    ConsumerConfig
}

// NewConsumer creates a new Kafka consumer group reader.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers configured")
	}
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("no kafka topics configured")
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "kafka2x-consumer-group"
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = 10e3 // 10KB
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10e6 // 10MB
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 250 * time.Millisecond
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 1000
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		GroupTopics:    cfg.Topics,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		QueueCapacity:  cfg.QueueCapacity,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
	}

	if cfg.StartOffset == kafka.FirstOffset {
		readerConfig.StartOffset = kafka.FirstOffset
	}

	reader := kafka.NewReader(readerConfig)
	return &Consumer{
		reader: reader,
		cfg:    cfg,
	}, nil
}

// ReadMessage fetches a single message from Kafka, automatically committing offset via group.
func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

// FetchMessage retrieves a message without immediately committing its offset.
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// CommitMessages marks processed message offsets as committed.
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

// Close gracefully closes the Kafka reader connection.
func (c *Consumer) Close() error {
	log.Printf("[Kafka Consumer] Closing consumer reader for topics: %v", c.cfg.Topics)
	return c.reader.Close()
}
