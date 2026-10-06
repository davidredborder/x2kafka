package listener

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"x2kafka/pkg/config"
	"x2kafka/pkg/decoder"
	"x2kafka/pkg/kafka"
	"x2kafka/pkg/security"
	"x2kafka/pkg/stats"
)

var (
	rfc5424Regex = regexp.MustCompile(`^<(\d{1,3})>1\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(?:(\[.*?\])|-)\s*(.*)$`)
	rfc3164Regex = regexp.MustCompile(`^<(\d{1,3})>(?:([A-Za-z]{3}\s+\d+\s+\d+:\d+:\d+|\d{4}-\d{2}-\d{2}T\S+)\s+)?(?:(\S+)\s+)?(?:([^\s\[:]+)(?:\[(\d+)\])?:?\s*)?(.*)$`)

	tagSdRegex  = regexp.MustCompile(`tag="([^"]+)"`)
	hashSdRegex = regexp.MustCompile(`hash="([^"]+)"`)

	facilityTexts = []string{
		"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
		"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "alert", "clock",
		"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
	}

	severityTexts = []string{
		"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
	}
)

type SyslogMessage struct {
	RawMessage         string                 `json:"raw_message"`
	Message            string                 `json:"message"`
	Pri                int                    `json:"pri"`
	PriText            string                 `json:"pri_text"`
	SyslogFacility     int                    `json:"syslogfacility"`
	SyslogFacilityText string                 `json:"syslogfacility_text"`
	SyslogSeverity     int                    `json:"syslogseverity"`
	SyslogSeverityText string                 `json:"syslogseverity_text"`
	ProtocolVersion    string                 `json:"protocol_version"`
	Timestamp          int64                  `json:"timestamp"`
	SyslogTimestamp    string                 `json:"syslog_timestamp,omitempty"`
	Hostname           string                 `json:"hostname"`
	FromHostIP         string                 `json:"fromhost_ip"`
	AppName            string                 `json:"app_name"`
	ProcID             string                 `json:"procid"`
	MsgID              string                 `json:"msgid"`
	StructuredData     string                 `json:"structured_data,omitempty"`
	Tag                string                 `json:"tag,omitempty"`
	Hash               string                 `json:"hash,omitempty"`
	SourceProto        string                 `json:"source_proto"`
	SourceAddr         string                 `json:"source_addr"`
	Enrichment         map[string]interface{} `json:"enrichment,omitempty"`
}

type SyslogListener struct {
	cfg          config.ListenerConfig
	decoder      *decoder.Decoder
	ipFilter     *security.IPFilter
	udpConn      *net.UDPConn
	tcpListener  net.Listener
	unixListener net.Listener
	unixPacket   net.PacketConn
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewSyslogListener(cfg config.ListenerConfig) *SyslogListener {
	return &SyslogListener{
		cfg:      cfg,
		decoder:  decoder.NewDecoder(),
		ipFilter: security.NewIPFilter(cfg.AllowedIPs, cfg.BlacklistIPs),
	}
}

func (s *SyslogListener) Name() string {
	if s.cfg.Name != "" {
		return s.cfg.Name
	}
	addr := s.cfg.Bind
	if s.cfg.Port > 0 {
		addr = fmt.Sprintf("%s:%d", s.cfg.Bind, s.cfg.Port)
	} else if s.cfg.Path != "" {
		addr = s.cfg.Path
	}
	return fmt.Sprintf("syslog-%s-%s", s.cfg.Proto, addr)
}

func (s *SyslogListener) Start(ctx context.Context, msgChan chan<- *kafka.Message) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	switch s.cfg.Proto {
	case "syslog_udp":
		addr := fmt.Sprintf("%s:%d", s.cfg.Bind, s.cfg.Port)
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			stats.IncListenerError(s.Name())
			return fmt.Errorf("failed to resolve syslog udp addr %s: %w", addr, err)
		}
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			stats.IncListenerError(s.Name())
			return fmt.Errorf("failed to listen on syslog udp %s: %w", addr, err)
		}

		if s.cfg.SocketBufferBytes > 0 {
			_ = conn.SetReadBuffer(s.cfg.SocketBufferBytes)
		}

		s.udpConn = conn

		numUDPReaders := 4
		for i := 0; i < numUDPReaders; i++ {
			s.wg.Add(1)
			go s.readUDP(conn, msgChan)
		}
		log.Printf("[%s] Listening for Syslog on UDP: %s (Buffer: %d MB)", s.Name(), addr, s.cfg.SocketBufferBytes/(1024*1024))

	case "syslog_tcp":
		addr := fmt.Sprintf("%s:%d", s.cfg.Bind, s.cfg.Port)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			stats.IncListenerError(s.Name())
			return fmt.Errorf("failed to listen on syslog tcp %s: %w", addr, err)
		}
		s.tcpListener = l
		s.wg.Add(1)
		go s.acceptTCP(msgChan)
		log.Printf("[%s] Listening for Syslog on TCP: %s", s.Name(), addr)

	case "syslog_unix":
		if s.cfg.Path != "" {
			_ = os.Remove(s.cfg.Path)
		}
		l, err := net.Listen("unix", s.cfg.Path)
		if err != nil {
			conn, err2 := net.ListenPacket("unixgram", s.cfg.Path)
			if err2 != nil {
				stats.IncListenerError(s.Name())
				return fmt.Errorf("failed to listen on syslog unix socket %s: %w", s.cfg.Path, err)
			}
			s.unixPacket = conn
			_ = os.Chmod(s.cfg.Path, 0666)
			s.wg.Add(1)
			go s.readUnixDatagram(conn, msgChan)
			log.Printf("[%s] Listening for Syslog on UNIX Datagram socket: %s", s.Name(), s.cfg.Path)
			return nil
		}
		s.unixListener = l
		_ = os.Chmod(s.cfg.Path, 0666)
		s.wg.Add(1)
		go s.acceptUnixStream(msgChan)
		log.Printf("[%s] Listening for Syslog on UNIX Stream socket: %s", s.Name(), s.cfg.Path)

	default:
		stats.IncListenerError(s.Name())
		return fmt.Errorf("unsupported syslog proto: %s", s.cfg.Proto)
	}

	return nil
}

func (s *SyslogListener) readUDP(conn *net.UDPConn, msgChan chan<- *kafka.Message) {
	defer s.wg.Done()
	buf := make([]byte, s.cfg.MaxMsgSize)

	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				stats.IncListenerError(s.Name())
				log.Printf("[%s] UDP Read error: %v", s.Name(), err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		if n > 0 {
			src := ""
			if remoteAddr != nil {
				src = remoteAddr.IP.String()
			}
			if !s.ipFilter.IsAllowed(src) {
				continue // Drop unauthorized sender IP
			}
			s.processSyslogMessage(buf[:n], "syslog_udp", src, msgChan)
		}
	}
}

func (s *SyslogListener) acceptTCP(msgChan chan<- *kafka.Message) {
	defer s.wg.Done()
	for {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				stats.IncListenerError(s.Name())
				log.Printf("[%s] TCP Accept error: %v", s.Name(), err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		remoteIP := conn.RemoteAddr().String()
		if host, _, err := net.SplitHostPort(remoteIP); err == nil {
			remoteIP = host
		}

		if !s.ipFilter.IsAllowed(remoteIP) {
			conn.Close()
			continue // Drop unauthorized connection
		}

		if s.cfg.SocketBufferBytes > 0 {
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				_ = tcpConn.SetReadBuffer(s.cfg.SocketBufferBytes)
			}
		}

		s.wg.Add(1)
		go s.handleTCPConnection(conn, remoteIP, msgChan)
	}
}

func (s *SyslogListener) handleTCPConnection(conn net.Conn, remoteIP string, msgChan chan<- *kafka.Message) {
	defer s.wg.Done()
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, s.cfg.MaxMsgSize)
	scanner.Buffer(buf, s.cfg.MaxMsgSize)

	for scanner.Scan() {
		s.processSyslogMessage(scanner.Bytes(), "syslog_tcp", remoteIP, msgChan)
	}
}

func (s *SyslogListener) acceptUnixStream(msgChan chan<- *kafka.Message) {
	defer s.wg.Done()
	for {
		conn, err := s.unixListener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				stats.IncListenerError(s.Name())
				log.Printf("[%s] Unix accept error: %v", s.Name(), err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleTCPConnection(conn, "unix", msgChan)
	}
}

func (s *SyslogListener) readUnixDatagram(conn net.PacketConn, msgChan chan<- *kafka.Message) {
	defer s.wg.Done()
	buf := make([]byte, s.cfg.MaxMsgSize)

	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				stats.IncListenerError(s.Name())
				log.Printf("[%s] Unix Datagram read error: %v", s.Name(), err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		if n > 0 {
			src := s.cfg.Path
			if addr != nil {
				src = addr.String()
			}
			s.processSyslogMessage(buf[:n], "syslog_unix", src, msgChan)
		}
	}
}

func (s *SyslogListener) processSyslogMessage(raw []byte, proto string, src string, msgChan chan<- *kafka.Message) {
	rawTrimmed := bytes.TrimSpace(raw)
	if len(rawTrimmed) == 0 {
		return
	}

	stats.IncMessagesReceived(s.Name(), len(rawTrimmed))

	if s.cfg.Mode == "raw" {
		msgChan <- &kafka.Message{
			Topic:     s.cfg.Topic,
			Value:     rawTrimmed,
			Timestamp: time.Now(),
		}
		return
	}

	syslogMsg := parseSyslog(string(rawTrimmed), proto, src, s.cfg.Enrichment)

	syslogBytes, _ := json.Marshal(syslogMsg)
	formatted, targetTopic, err := s.decoder.FormatPayloadWithConfigAndTopic(syslogBytes, s.cfg, proto, src)
	if err != nil || len(formatted) == 0 {
		formatted = syslogBytes
	}

	topic := targetTopic
	if topic == "" {
		topic = s.cfg.Topic
	}

	msgChan <- &kafka.Message{
		Topic:     topic,
		Value:     formatted,
		Timestamp: time.Now(),
	}
}

func parseSyslog(raw string, proto string, src string, enrichment map[string]interface{}) *SyslogMessage {
	now := time.Now().Unix()
	msg := &SyslogMessage{
		RawMessage:         raw,
		Message:            raw,
		Pri:                13,
		PriText:            "user.notice",
		SyslogFacility:     1,
		SyslogFacilityText: "user",
		SyslogSeverity:     5,
		SyslogSeverityText: "notice",
		ProtocolVersion:    "0",
		Timestamp:          now,
		FromHostIP:         src,
		SourceProto:        proto,
		SourceAddr:         src,
		Enrichment:         enrichment,
	}

	if rfc5424Regex.MatchString(raw) {
		matches := rfc5424Regex.FindStringSubmatch(raw)
		if len(matches) >= 9 {
			pri, _ := strconv.Atoi(matches[1])
			msg.Pri = pri
			msg.SyslogFacility = pri / 8
			msg.SyslogSeverity = pri % 8
			if msg.SyslogFacility >= 0 && msg.SyslogFacility < len(facilityTexts) {
				msg.SyslogFacilityText = facilityTexts[msg.SyslogFacility]
			}
			if msg.SyslogSeverity >= 0 && msg.SyslogSeverity < len(severityTexts) {
				msg.SyslogSeverityText = severityTexts[msg.SyslogSeverity]
			}
			msg.PriText = fmt.Sprintf("%s.%s", msg.SyslogFacilityText, msg.SyslogSeverityText)

			msg.ProtocolVersion = "1"
			msg.SyslogTimestamp = matches[2]
			msg.Hostname = matches[3]
			msg.AppName = matches[4]
			msg.ProcID = matches[5]
			msg.MsgID = matches[6]
			msg.StructuredData = matches[7]
			msg.Message = matches[8]

			if msg.StructuredData != "" {
				if tagMatch := tagSdRegex.FindStringSubmatch(msg.StructuredData); len(tagMatch) > 1 {
					msg.Tag = tagMatch[1]
				}
				if hashMatch := hashSdRegex.FindStringSubmatch(msg.StructuredData); len(hashMatch) > 1 {
					msg.Hash = hashMatch[1]
				}
			}
			return msg
		}
	}

	if rfc3164Regex.MatchString(raw) {
		matches := rfc3164Regex.FindStringSubmatch(raw)
		if len(matches) >= 7 {
			pri, _ := strconv.Atoi(matches[1])
			msg.Pri = pri
			msg.SyslogFacility = pri / 8
			msg.SyslogSeverity = pri % 8
			if msg.SyslogFacility >= 0 && msg.SyslogFacility < len(facilityTexts) {
				msg.SyslogFacilityText = facilityTexts[msg.SyslogFacility]
			}
			if msg.SyslogSeverity >= 0 && msg.SyslogSeverity < len(severityTexts) {
				msg.SyslogSeverityText = severityTexts[msg.SyslogSeverity]
			}
			msg.PriText = fmt.Sprintf("%s.%s", msg.SyslogFacilityText, msg.SyslogSeverityText)

			msg.SyslogTimestamp = matches[2]
			msg.Hostname = matches[3]
			msg.AppName = matches[4]
			msg.ProcID = matches[5]
			msg.Message = strings.TrimSpace(matches[6])
			return msg
		}
	}

	return msg
}

func (s *SyslogListener) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.udpConn != nil {
		_ = s.udpConn.Close()
	}
	if s.tcpListener != nil {
		_ = s.tcpListener.Close()
	}
	if s.unixListener != nil {
		_ = s.unixListener.Close()
	}
	if s.unixPacket != nil {
		_ = s.unixPacket.Close()
	}
	s.wg.Wait()
	if s.cfg.Proto == "syslog_unix" && s.cfg.Path != "" {
		_ = os.Remove(s.cfg.Path)
	}
	return nil
}
