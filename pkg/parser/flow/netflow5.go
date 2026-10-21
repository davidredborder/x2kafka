package flow

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// ParseNetFlowV5 parses a NetFlow v5 datagram into JSONFlowRecord slices.
func ParseNetFlowV5(data []byte, exporterIP string, cfg *ParserConfig) ([]*JSONFlowRecord, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("netflow v5 packet too short: %d bytes (min 24)", len(data))
	}

	version := binary.BigEndian.Uint16(data[0:2])
	if version != 5 {
		return nil, fmt.Errorf("invalid netflow version %d (expected 5)", version)
	}

	count := int(binary.BigEndian.Uint16(data[2:4]))
	sysUptime := binary.BigEndian.Uint32(data[4:8])
	unixSecs := int64(binary.BigEndian.Uint32(data[8:12]))
	flowSequence := binary.BigEndian.Uint32(data[16:20])
	samplingInterval := binary.BigEndian.Uint16(data[22:24])
	samplingRate := uint32(samplingInterval & 0x3FFF) // bottom 14 bits

	expectedLen := 24 + count*48
	if len(data) < expectedLen {
		return nil, fmt.Errorf("netflow v5 payload truncated: have %d bytes, need %d for %d records", len(data), expectedLen, count)
	}

	records := make([]*JSONFlowRecord, 0, count)
	now := time.Now().Unix()

	for i := 0; i < count; i++ {
		offset := 24 + i*48
		recBytes := data[offset : offset+48]

		srcIP := net.IP(recBytes[0:4]).String()
		dstIP := net.IP(recBytes[4:8]).String()
		pkts := uint64(binary.BigEndian.Uint32(recBytes[16:20]))
		bytesCount := uint64(binary.BigEndian.Uint32(recBytes[20:24]))
		firstUptime := binary.BigEndian.Uint32(recBytes[24:28])
		lastUptime := binary.BigEndian.Uint32(recBytes[28:32])
		srcPort := binary.BigEndian.Uint16(recBytes[32:34])
		dstPort := binary.BigEndian.Uint16(recBytes[34:36])
		tcpFlags := recBytes[37]
		proto := recBytes[38]

		if cfg != nil && cfg.RenormalizeSampling && samplingRate > 1 {
			pkts *= uint64(samplingRate)
			bytesCount *= uint64(samplingRate)
		}

		// Calculate start/end unix timestamps from sysUptime offset
		var startTime, endTime, duration int64
		if sysUptime >= firstUptime {
			startTime = unixSecs - int64((sysUptime-firstUptime)/1000)
		} else {
			startTime = unixSecs
		}
		if sysUptime >= lastUptime {
			endTime = unixSecs - int64((sysUptime-lastUptime)/1000)
		} else {
			endTime = unixSecs
		}
		if lastUptime >= firstUptime {
			duration = int64(lastUptime - firstUptime)
		}

		inputIf := uint32(binary.BigEndian.Uint16(recBytes[8:10]))
		outputIf := uint32(binary.BigEndian.Uint16(recBytes[10:12]))

		rec := &JSONFlowRecord{
			Type:                    "netflowv5",
			FlowSequence:            fmt.Sprintf("%d", flowSequence),
			IPProtocolVersion:       4,
			L4Proto:                 proto,
			Direction:               "upstream",
			LanInterface:            inputIf,
			LanInterfaceName:        fmt.Sprintf("%d", inputIf),
			LanInterfaceDescription: fmt.Sprintf("%d", inputIf),
			WanInterface:            outputIf,
			WanInterfaceName:        fmt.Sprintf("%d", outputIf),
			WanInterfaceDescription: fmt.Sprintf("%d", outputIf),
			TCPFlags:                FormatTCPFlags(tcpFlags),
			Bytes:                   bytesCount,
			Pkts:                    pkts,
			LanIP:                   srcIP,
			WanIP:                   dstIP,
			LanL4Port:               srcPort,
			WanL4Port:               dstPort,
			TOS:                     recBytes[39],
			SamplingRate:            samplingRate,
			Timestamp:               now,
			StartTime:               startTime,
			EndTime:                 endTime,
			Duration:                duration,
			SensorIP:                exporterIP,
		}

		records = append(records, rec)
	}

	return records, nil
}
