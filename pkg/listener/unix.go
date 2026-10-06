package listener

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"x2kafka/pkg/config"
	"x2kafka/pkg/decoder"
	"x2kafka/pkg/kafka"
	"x2kafka/pkg/stats"
)

type UnixListener struct {
	cfg          config.ListenerConfig
	decoder      *decoder.Decoder
	listener     net.Listener
	connDatagram net.PacketConn
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewUnixListener(cfg config.ListenerConfig) *UnixListener {
	return &UnixListener{
		cfg:     cfg,
		decoder: decoder.NewDecoder(),
	}
}

func (u *UnixListener) Name() string {
	if u.cfg.Name != "" {
		return u.cfg.Name
	}
	return fmt.Sprintf("unix-%s-%s", u.cfg.Proto, u.cfg.Path)
}

func (u *UnixListener) Start(ctx context.Context, msgChan chan<- *kafka.Message) error {
	u.ctx, u.cancel = context.WithCancel(ctx)

	if u.cfg.Path != "" {
		_ = os.Remove(u.cfg.Path)
	}

	if u.cfg.Proto == "unixgram" {
		conn, err := net.ListenPacket("unixgram", u.cfg.Path)
		if err != nil {
			stats.IncListenerError(u.Name())
			return fmt.Errorf("failed to listen on unixgram socket %s: %w", u.cfg.Path, err)
		}
		u.connDatagram = conn
		_ = os.Chmod(u.cfg.Path, 0666)

		u.wg.Add(1)
		go u.readDatagram(conn, msgChan)
		log.Printf("[%s] Listening on UNIX Datagram socket: %s", u.Name(), u.cfg.Path)
		return nil
	}

	l, err := net.Listen("unix", u.cfg.Path)
	if err != nil {
		stats.IncListenerError(u.Name())
		return fmt.Errorf("failed to listen on unix stream socket %s: %w", u.cfg.Path, err)
	}
	u.listener = l
	_ = os.Chmod(u.cfg.Path, 0666)

	u.wg.Add(1)
	go u.acceptStream(msgChan)
	log.Printf("[%s] Listening on UNIX Stream socket: %s", u.Name(), u.cfg.Path)
	return nil
}

func (u *UnixListener) acceptStream(msgChan chan<- *kafka.Message) {
	defer u.wg.Done()

	for {
		conn, err := u.listener.Accept()
		if err != nil {
			select {
			case <-u.ctx.Done():
				return
			default:
				stats.IncListenerError(u.Name())
				log.Printf("[%s] Accept error: %v", u.Name(), err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		u.wg.Add(1)
		go u.handleConnection(conn, msgChan)
	}
}

func (u *UnixListener) handleConnection(conn net.Conn, msgChan chan<- *kafka.Message) {
	defer u.wg.Done()
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, u.cfg.MaxMsgSize)
	scanner.Buffer(buf, u.cfg.MaxMsgSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		stats.IncMessagesReceived(u.Name(), len(line))

		formatted, err := u.decoder.FormatPayloadWithConfig(line, u.cfg, u.cfg.Proto, u.cfg.Path)
		if err != nil || len(formatted) == 0 {
			continue
		}

		msgChan <- &kafka.Message{
			Topic:     u.cfg.Topic,
			Value:     formatted,
			Timestamp: time.Now(),
		}
	}

	if err := scanner.Err(); err != nil {
		select {
		case <-u.ctx.Done():
			return
		default:
			stats.IncListenerError(u.Name())
			log.Printf("[%s] Connection scan error: %v", u.Name(), err)
		}
	}
}

func (u *UnixListener) readDatagram(conn net.PacketConn, msgChan chan<- *kafka.Message) {
	defer u.wg.Done()

	buf := make([]byte, u.cfg.MaxMsgSize)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-u.ctx.Done():
				return
			default:
				stats.IncListenerError(u.Name())
				log.Printf("[%s] Datagram read error: %v", u.Name(), err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			stats.IncMessagesReceived(u.Name(), n)

			addrStr := u.cfg.Path
			if addr != nil {
				addrStr = addr.String()
			}

			formatted, err := u.decoder.FormatPayloadWithConfig(data, u.cfg, u.cfg.Proto, addrStr)
			if err != nil || len(formatted) == 0 {
				continue
			}

			msgChan <- &kafka.Message{
				Topic:     u.cfg.Topic,
				Value:     formatted,
				Timestamp: time.Now(),
			}
		}
	}
}

func (u *UnixListener) Stop() error {
	if u.cancel != nil {
		u.cancel()
	}
	if u.listener != nil {
		_ = u.listener.Close()
	}
	if u.connDatagram != nil {
		_ = u.connDatagram.Close()
	}
	u.wg.Wait()
	if u.cfg.Path != "" {
		_ = os.Remove(u.cfg.Path)
	}
	return nil
}
