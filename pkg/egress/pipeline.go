package egress

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	pkgkafka "x2kafka/pkg/kafka"
	"x2kafka/pkg/stats"
)

// PipelineConfig defines batching, concurrency, and rate limiting for Kafka egress.
type PipelineConfig struct {
	Debug         bool `json:"debug,omitempty" yaml:"debug,omitempty"`
	Workers       int  `json:"workers" yaml:"workers"`
	BatchSize     int  `json:"batch_size" yaml:"batch_size"`
	TimeoutMillis int  `json:"timeout_ms" yaml:"timeout_ms"`
	Retries       int  `json:"retries" yaml:"retries"`
	BackoffSec    int  `json:"backoff_sec" yaml:"backoff_sec"`
	RateLimit     int  `json:"rate_limit" yaml:"rate_limit"` // Max messages per second (0 for unlimited)
}

// Pipeline bridges a Kafka consumer with an HTTPSender using worker pools and batching.
type Pipeline struct {
	consumer   *pkgkafka.Consumer
	sender     *HTTPSender
	cfg        PipelineConfig
	msgQueue   chan kafka.Message
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewPipeline creates a new egress pipeline.
func NewPipeline(consumer *pkgkafka.Consumer, sender *HTTPSender, cfg PipelineConfig) *Pipeline {
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.TimeoutMillis <= 0 {
		cfg.TimeoutMillis = 100
	}
	if cfg.BackoffSec <= 0 {
		cfg.BackoffSec = 2
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		consumer: consumer,
		sender:   sender,
		cfg:      cfg,
		msgQueue: make(chan kafka.Message, cfg.BatchSize*cfg.Workers*4),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start launches the consumer loop and batch sender worker goroutines.
func (p *Pipeline) Start() {
	log.Printf("[Pipeline] Starting %d workers (BatchSize: %d, Timeout: %dms, Retries: %d)",
		p.cfg.Workers, p.cfg.BatchSize, p.cfg.TimeoutMillis, p.cfg.Retries)

	// 1. Start batch sender workers
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	// 2. Start Kafka reader loop
	p.wg.Add(1)
	go p.readerLoop()
}

func (p *Pipeline) readerLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			msg, err := p.consumer.FetchMessage(p.ctx)
			if err != nil {
				if p.ctx.Err() != nil {
					return
				}
				log.Printf("[Pipeline Reader] FetchMessage error: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			stats.IncMessagesReceived("kafka_consumer", len(msg.Value))

			select {
			case p.msgQueue <- msg:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

func (p *Pipeline) worker(id int) {
	defer p.wg.Done()

	rawMsgs := make([]kafka.Message, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(time.Duration(p.cfg.TimeoutMillis) * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(rawMsgs) == 0 {
			return
		}

		// Group messages by topic while preserving topic encounter order
		var topics []string
		byTopicRecords := make(map[string][][]byte)
		byTopicMsgs := make(map[string][]kafka.Message)

		for _, m := range rawMsgs {
			t := m.Topic
			if _, exists := byTopicRecords[t]; !exists {
				topics = append(topics, t)
			}
			byTopicRecords[t] = append(byTopicRecords[t], m.Value)
			byTopicMsgs[t] = append(byTopicMsgs[t], m)
		}

		for _, topic := range topics {
			records := byTopicRecords[topic]
			tMsgs := byTopicMsgs[topic]

			var err error
			attempts := 0
			maxAttempts := p.cfg.Retries + 1
			if p.cfg.Retries < 0 {
				maxAttempts = 1000000 // Infinite
			}

			for attempts < maxAttempts {
				attempts++
				sendCtx := p.ctx
				if sendCtx.Err() != nil {
					var cancelFlush context.CancelFunc
					sendCtx, cancelFlush = context.WithTimeout(context.Background(), 5*time.Second)
					err = p.sender.SendBatchToTopic(sendCtx, topic, records)
					cancelFlush()
					break
				}
				err = p.sender.SendBatchToTopic(sendCtx, topic, records)
				if err == nil {
					break
				}

				stats.IncListenerError("http_sender")
				log.Printf("[Worker %d] SendBatch error for topic %q (attempt %d/%d): %v", id, topic, attempts, maxAttempts, err)

				select {
				case <-p.ctx.Done():
					return
				case <-time.After(time.Duration(p.cfg.BackoffSec) * time.Second):
				}
			}

			if err == nil {
				// Commit Kafka offsets after confirmed successful HTTP delivery!
				commitCtx := p.ctx
				if commitCtx.Err() != nil {
					var cancelCommit context.CancelFunc
					commitCtx, cancelCommit = context.WithTimeout(context.Background(), 5*time.Second)
					defer cancelCommit()
				}
				if commitErr := p.consumer.CommitMessages(commitCtx, tMsgs...); commitErr != nil && p.cfg.Debug {
					log.Printf("[Worker %d] CommitMessages warning for topic %q: %v", id, topic, commitErr)
				}
				totalBytes := 0
				for _, b := range records {
					totalBytes += len(b)
				}
				stats.AddMessagesSentKafka(int64(len(records)), int64(totalBytes))
				if p.cfg.Debug {
					log.Printf("[Worker %d] Successfully delivered batch of %d messages (%d bytes) for topic %q", id, len(records), totalBytes, topic)
				}
			} else {
				stats.IncMessagesFailedKafka()
				log.Printf("[Worker %d] Failed to deliver batch for topic %q after %d attempts: %v", id, topic, attempts, err)
			}
		}

		rawMsgs = rawMsgs[:0]
	}

	for {
		select {
		case <-p.ctx.Done():
			flush()
			return

		case <-ticker.C:
			flush()

		case msg, ok := <-p.msgQueue:
			if !ok {
				flush()
				return
			}
			rawMsgs = append(rawMsgs, msg)
			if len(rawMsgs) >= p.cfg.BatchSize {
				flush()
			}
		}
	}
}

// Stop gracefully shuts down the pipeline.
func (p *Pipeline) Stop() {
	log.Printf("[Pipeline] Stopping pipeline...")
	p.cancel()
	p.wg.Wait()
	_ = p.consumer.Close()
	log.Printf("[Pipeline] Stopped successfully.")
}
