package stats

import (
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type ListenerStats struct {
	Name             string `json:"name"`
	Proto            string `json:"proto"`
	MessagesReceived int64  `json:"messages_received"`
	BytesReceived    int64  `json:"bytes_received"`
	LastMessageTime  int64  `json:"last_message_timestamp_ms"`
	ErrorCount       int64  `json:"error_count"`
}

type Stats struct {
	UptimeSeconds      int64           `json:"uptime_seconds"`
	MessagesReceived   int64           `json:"messages_received"`
	MessagesSentKafka  int64           `json:"messages_sent_kafka"`
	MessagesFailedKafka int64          `json:"messages_failed_kafka"`
	BytesReceived      int64           `json:"bytes_received"`
	BytesSentKafka     int64           `json:"bytes_sent_kafka"`
	AllocBytes         uint64          `json:"alloc_bytes"`
	SysBytes           uint64          `json:"sys_bytes"`
	NumGC              uint32          `json:"num_gc"`
	Listeners          []ListenerStats `json:"listeners,omitempty"`
}

var (
	startTime           = time.Now()
	messagesReceived    int64
	messagesSentKafka   int64
	messagesFailedKafka int64
	bytesReceived       int64
	bytesSentKafka      int64

	listenerStatsMap = make(map[string]*ListenerStats)
	listenerMu       sync.RWMutex
)

func RegisterListener(name, proto string) {
	listenerMu.Lock()
	defer listenerMu.Unlock()
	listenerStatsMap[name] = &ListenerStats{
		Name:  name,
		Proto: proto,
	}
}

func IncMessagesReceived(listenerName string, byteCount int) {
	atomic.AddInt64(&messagesReceived, 1)
	atomic.AddInt64(&bytesReceived, int64(byteCount))

	listenerMu.RLock()
	ls, ok := listenerStatsMap[listenerName]
	listenerMu.RUnlock()

	if ok && ls != nil {
		atomic.AddInt64(&ls.MessagesReceived, 1)
		atomic.AddInt64(&ls.BytesReceived, int64(byteCount))
		atomic.StoreInt64(&ls.LastMessageTime, time.Now().UnixNano()/1e6)
	}
}

func IncListenerError(listenerName string) {
	listenerMu.RLock()
	ls, ok := listenerStatsMap[listenerName]
	listenerMu.RUnlock()

	if ok && ls != nil {
		atomic.AddInt64(&ls.ErrorCount, 1)
	}
}

func IncMessagesSentKafka(byteCount int) {
	atomic.AddInt64(&messagesSentKafka, 1)
	atomic.AddInt64(&bytesSentKafka, int64(byteCount))
}

func AddMessagesSentKafka(msgCount, byteCount int64) {
	atomic.AddInt64(&messagesSentKafka, msgCount)
	atomic.AddInt64(&bytesSentKafka, byteCount)
}

func IncMessagesFailedKafka() {
	atomic.AddInt64(&messagesFailedKafka, 1)
}

func GetStats() Stats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	listenerMu.RLock()
	var listeners []ListenerStats
	for _, ls := range listenerStatsMap {
		if ls != nil {
			listeners = append(listeners, ListenerStats{
				Name:             ls.Name,
				Proto:            ls.Proto,
				MessagesReceived: atomic.LoadInt64(&ls.MessagesReceived),
				BytesReceived:    atomic.LoadInt64(&ls.BytesReceived),
				LastMessageTime:  atomic.LoadInt64(&ls.LastMessageTime),
				ErrorCount:       atomic.LoadInt64(&ls.ErrorCount),
			})
		}
	}
	listenerMu.RUnlock()

	return Stats{
		UptimeSeconds:       int64(time.Since(startTime).Seconds()),
		MessagesReceived:    atomic.LoadInt64(&messagesReceived),
		MessagesSentKafka:   atomic.LoadInt64(&messagesSentKafka),
		MessagesFailedKafka: atomic.LoadInt64(&messagesFailedKafka),
		BytesReceived:       atomic.LoadInt64(&bytesReceived),
		BytesSentKafka:      atomic.LoadInt64(&bytesSentKafka),
		AllocBytes:          m.Alloc,
		SysBytes:            m.Sys,
		NumGC:               m.NumGC,
		Listeners:           listeners,
	}
}

// StatsHandler returns an http.HandlerFunc to serve stats in JSON format.
func StatsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := GetStats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	}
}
