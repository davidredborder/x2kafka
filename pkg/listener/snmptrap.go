package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gosnmp/gosnmp"
	"x2kafka/pkg/config"
	"x2kafka/pkg/decoder"
	"x2kafka/pkg/kafka"
	"x2kafka/pkg/mib"
	"x2kafka/pkg/security"
	"x2kafka/pkg/stats"
)

type SNMPTrapMessage struct {
	TrapOID      string                 `json:"trap_oid"`
	TrapName     string                 `json:"trap_name,omitempty"`
	Community    string                 `json:"community,omitempty"`
	AgentAddr    string                 `json:"agent_address,omitempty"`
	Enterprise   string                 `json:"enterprise,omitempty"`
	GenericTrap  int                    `json:"generic_trap,omitempty"`
	SpecificTrap int                    `json:"specific_trap,omitempty"`
	Variables    map[string]interface{} `json:"variables"`
	SrcIP        string                 `json:"src_ip"`
	SrcPort      int                    `json:"src_port"`
	SourceProto  string                 `json:"source_proto"`
	SourceAddr   string                 `json:"source_addr"`
	Timestamp    int64                  `json:"timestamp"`
	Enrichment   map[string]interface{} `json:"enrichment,omitempty"`
}

type SNMPTrapListener struct {
	cfg      config.ListenerConfig
	decoder  *decoder.Decoder
	ipFilter *security.IPFilter
	listener *gosnmp.TrapListener
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	tcpListener net.Listener
}

func NewSNMPTrapListener(cfg config.ListenerConfig) *SNMPTrapListener {
	return &SNMPTrapListener{
		cfg:      cfg,
		decoder:  decoder.NewDecoder(),
		ipFilter: security.NewIPFilter(cfg.AllowedIPs, cfg.BlacklistIPs),
	}
}

func (s *SNMPTrapListener) Name() string {
	if s.cfg.Name != "" {
		return s.cfg.Name
	}
	port := s.cfg.Port
	if port <= 0 {
		port = 162
	}
	return fmt.Sprintf("%s-%s:%d", s.cfg.Proto, s.cfg.Bind, port)
}

func (s *SNMPTrapListener) Start(ctx context.Context, msgChan chan<- *kafka.Message) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	port := uint16(s.cfg.Port)
	if port == 0 {
		port = 162
	}
	bindAddr := fmt.Sprintf("%s:%d", s.cfg.Bind, port)

	if s.cfg.Proto == "snmptrap_tcp" {
		l, err := net.Listen("tcp", bindAddr)
		if err != nil {
			stats.IncListenerError(s.Name())
			return fmt.Errorf("failed to listen on snmptrap tcp %s: %w", bindAddr, err)
		}
		s.tcpListener = l
		s.wg.Add(1)
		go s.acceptTCPTraps(msgChan)
		log.Printf("[%s] Listening for Direct SNMP Traps on TCP: %s", s.Name(), bindAddr)
		return nil
	}

	tl := gosnmp.NewTrapListener()
	tl.OnNewTrap = func(packet *gosnmp.SnmpPacket, addr *net.UDPAddr) {
		s.handleTrapPacket(packet, addr, msgChan)
	}

	s.listener = tl

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Printf("[%s] Listening for Direct SNMP Traps on UDP: %s", s.Name(), bindAddr)
		err := tl.Listen(bindAddr)
		if err != nil {
			stats.IncListenerError(s.Name())
			log.Printf("[%s] SNMP Trap listener error: %v", s.Name(), err)
		}
	}()

	return nil
}

func (s *SNMPTrapListener) acceptTCPTraps(msgChan chan<- *kafka.Message) {
	defer s.wg.Done()
	for {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				stats.IncListenerError(s.Name())
				log.Printf("[%s] TCP Trap accept error: %v", s.Name(), err)
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
			continue
		}

		s.wg.Add(1)
		go s.handleTCPConnection(conn, remoteIP, msgChan)
	}
}

func (s *SNMPTrapListener) handleTCPConnection(conn net.Conn, remoteIP string, msgChan chan<- *kafka.Message) {
	defer s.wg.Done()
	defer conn.Close()

	buf := make([]byte, s.cfg.MaxMsgSize)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		if n > 0 {
			stats.IncMessagesReceived(s.Name(), n)
			formatted, targetTopic, err := s.decoder.FormatPayloadWithConfigAndTopic(buf[:n], s.cfg, "snmptrap_tcp", remoteIP)
			if err == nil && len(formatted) > 0 {
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
		}
	}
}

func (s *SNMPTrapListener) handleTrapPacket(packet *gosnmp.SnmpPacket, addr *net.UDPAddr, msgChan chan<- *kafka.Message) {
	if packet == nil {
		return
	}

	srcIP := ""
	if addr != nil {
		srcIP = addr.IP.String()
	}

	if !s.ipFilter.IsAllowed(srcIP) {
		return // Drop unauthorized trap sender IP
	}

	varsMap := make(map[string]interface{})
	trapOID := ""
	trapName := ""

	for _, pdu := range packet.Variables {
		oidStr := pdu.Name
		resolvedName := mib.ResolveOID(oidStr)

		var val interface{} = pdu.Value
		if bytesVal, ok := pdu.Value.([]byte); ok {
			val = string(bytesVal)
		}

		if oidStr == ".1.3.6.1.6.3.1.1.4.1.0" || resolvedName == "snmpTrapOID.0" {
			if strVal, ok := val.(string); ok {
				trapOID = strVal
				trapName = mib.ResolveOID(trapOID)
			}
		}

		if resolvedName != "" && resolvedName != oidStr {
			varsMap[resolvedName] = val
		} else {
			varsMap[oidStr] = val
		}
	}

	if trapOID == "" && packet.Enterprise != "" {
		trapOID = packet.Enterprise
		trapName = mib.ResolveOID(trapOID)
	}

	msgMap := make(map[string]interface{})

	// snmptrapd standard root schema fields
	if trapName != "" {
		msgMap["oid"] = trapName
	} else if trapOID != "" {
		msgMap["oid"] = trapOID
	}

	protoPrefix := "UDP"
	if strings.Contains(s.cfg.Proto, "tcp") {
		protoPrefix = "TCP"
	}

	var srcAddrStr string
	if addr != nil {
		srcAddrStr = fmt.Sprintf("[%s]:%d", addr.IP.String(), addr.Port)
	} else {
		srcAddrStr = fmt.Sprintf("[%s]", srcIP)
	}

	dstPort := s.cfg.Port
	if dstPort <= 0 {
		dstPort = 162
	}
	dstAddrStr := fmt.Sprintf("[%s]:%d", s.cfg.Bind, dstPort)

	srcPort := 0
	if addr != nil {
		srcPort = addr.Port
	}

	msgMap["host"] = srcIP
	msgMap["reqid"] = packet.RequestID
	msgMap["version"] = int(packet.Version)
	msgMap["command"] = int(packet.PDUType)
	msgMap["community"] = packet.Community
	msgMap["transport"] = fmt.Sprintf("%s: %s->%s", protoPrefix, srcAddrStr, dstAddrStr)
	msgMap["security_model"] = int(packet.SecurityModel)
	msgMap["timestamp"] = fmt.Sprintf("%d", time.Now().Unix())
	msgMap["src_ip"] = srcIP
	msgMap["src_port"] = srcPort
	msgMap["source_proto"] = s.cfg.Proto
	msgMap["source_addr"] = srcIP

	if trapOID != "" {
		msgMap["trap_oid"] = trapOID
	}
	if trapName != "" {
		msgMap["trap_name"] = trapName
	}

	for k, v := range s.cfg.Enrichment {
		msgMap[k] = v
	}

	// Flatten all extracted MIB variables into root object
	for k, v := range varsMap {
		if _, exists := msgMap[k]; !exists {
			msgMap[k] = v
		}
	}

	msgBytes, err := json.Marshal(msgMap)
	if err != nil {
		stats.IncListenerError(s.Name())
		return
	}

	stats.IncMessagesReceived(s.Name(), len(msgBytes))

	formatted, targetTopic, err := s.decoder.FormatPayloadWithConfigAndTopic(msgBytes, s.cfg, s.cfg.Proto, srcIP)
	if err != nil || len(formatted) == 0 {
		formatted = msgBytes
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

func (s *SNMPTrapListener) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.listener != nil {
		s.listener.Close()
	}
	if s.tcpListener != nil {
		_ = s.tcpListener.Close()
	}
	s.wg.Wait()
	return nil
}
