package listener

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"x2kafka/pkg/config"
	"x2kafka/pkg/decoder"
	"x2kafka/pkg/kafka"
	"x2kafka/pkg/stats"
)

type FifoListener struct {
	cfg     config.ListenerConfig
	decoder *decoder.Decoder
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewFifoListener(cfg config.ListenerConfig) *FifoListener {
	return &FifoListener{
		cfg:     cfg,
		decoder: decoder.NewDecoder(),
	}
}

func (f *FifoListener) Name() string {
	if f.cfg.Name != "" {
		return f.cfg.Name
	}
	return fmt.Sprintf("fifo-%s", f.cfg.Path)
}

func (f *FifoListener) Start(ctx context.Context, msgChan chan<- *kafka.Message) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	fi, err := os.Stat(f.cfg.Path)
	if os.IsNotExist(err) {
		err = syscall.Mkfifo(f.cfg.Path, 0666)
		if err != nil {
			stats.IncListenerError(f.Name())
			return fmt.Errorf("failed to create fifo at %s: %w", f.cfg.Path, err)
		}
	} else if err == nil && fi.Mode()&os.ModeNamedPipe == 0 {
		stats.IncListenerError(f.Name())
		return fmt.Errorf("path %s exists but is not a named pipe (fifo)", f.cfg.Path)
	}

	_ = os.Chmod(f.cfg.Path, 0666)

	f.wg.Add(1)
	go f.readFifoLoop(msgChan)
	log.Printf("[%s] Listening on FIFO pipe: %s", f.Name(), f.cfg.Path)
	return nil
}

func (f *FifoListener) readFifoLoop(msgChan chan<- *kafka.Message) {
	defer f.wg.Done()

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		file, err := os.OpenFile(f.cfg.Path, os.O_RDONLY, 0666)
		if err != nil {
			select {
			case <-f.ctx.Done():
				return
			default:
				stats.IncListenerError(f.Name())
				log.Printf("[%s] Error opening FIFO: %v", f.Name(), err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}

		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, f.cfg.MaxMsgSize)
		scanner.Buffer(buf, f.cfg.MaxMsgSize)

		for scanner.Scan() {
			line := scanner.Bytes()
			stats.IncMessagesReceived(f.Name(), len(line))

			formatted, err := f.decoder.FormatPayloadWithConfig(line, f.cfg, "fifo", f.cfg.Path)
			if err != nil || len(formatted) == 0 {
				continue
			}

			msgChan <- &kafka.Message{
				Topic:     f.cfg.Topic,
				Value:     formatted,
				Timestamp: time.Now(),
			}
		}

		_ = file.Close()
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *FifoListener) Stop() error {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
	return nil
}
