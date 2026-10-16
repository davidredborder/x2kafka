package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"x2kafka/pkg/stats"
)

type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// ProducerConfig defines the configuration parameters for the Kafka producer.
type ProducerConfig struct {
	Brokers        []string
	Topic          string
	BatchSize      int
	BatchTimeoutMS int
	Async          bool
	Compression    string
}

type Producer struct {
	writers map[string]*kafka.Writer
	brokers []string
	cfg     ProducerConfig
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		writers: make(map[string]*kafka.Writer),
		brokers: cfg.Brokers,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}

	if cfg.Topic != "" {
		p.getWriter(cfg.Topic)
	}

	return p, nil
}

func (p *Producer) getWriter(topic string) *kafka.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()

	if w, ok := p.writers[topic]; ok {
		return w
	}

	batchSize := p.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	batchTimeout := time.Duration(p.cfg.BatchTimeoutMS) * time.Millisecond
	if batchTimeout <= 0 {
		batchTimeout = 10 * time.Millisecond
	}

	var codec kafka.Compression
	switch p.cfg.Compression {
	case "lz4":
		codec = compress.Lz4
	case "gzip":
		codec = compress.Gzip
	case "snappy":
		codec = compress.Snappy
	case "zstd":
		codec = compress.Zstd
	default:
		codec = 0 // No compression
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(p.brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		Async:        p.cfg.Async,
		Compression:  codec,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				stats.IncMessagesFailedKafka()
				log.Printf("[Kafka Producer Error] Topic %s failed to write %d messages: %v", topic, len(messages), err)
			} else {
				for _, msg := range messages {
					stats.IncMessagesSentKafka(len(msg.Value))
				}
			}
		},
	}

	p.writers[topic] = w
	return w
}

func (p *Producer) Produce(msg *Message) error {
	topic := msg.Topic
	if topic == "" {
		topic = p.cfg.Topic
	}

	w := p.getWriter(topic)

	var kHeaders []kafka.Header
	for k, v := range msg.Headers {
		kHeaders = append(kHeaders, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	kafkaMsg := kafka.Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   kHeaders,
		Time:      ts,
	}

	err := w.WriteMessages(p.ctx, kafkaMsg)
	if err != nil {
		stats.IncMessagesFailedKafka()
		return fmt.Errorf("failed to write message to topic %s: %w", topic, err)
	}

	return nil
}

func (p *Producer) Close() error {
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for topic, w := range p.writers {
		if err := w.Close(); err != nil {
			log.Printf("Error closing kafka writer for topic %s: %v", topic, err)
			lastErr = err
		}
	}
	return lastErr
}
