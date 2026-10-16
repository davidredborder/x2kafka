package listener

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"x2kafka/pkg/config"
	"x2kafka/pkg/decoder"
	"x2kafka/pkg/kafka"
	"x2kafka/pkg/security"
	"x2kafka/pkg/stats"
)

type HTTPListener struct {
	cfg      config.ListenerConfig
	decoder  *decoder.Decoder
	ipFilter *security.IPFilter
	server   *http.Server
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewHTTPListener(cfg config.ListenerConfig) *HTTPListener {
	return &HTTPListener{
		cfg:      cfg,
		decoder:  decoder.NewDecoder(),
		ipFilter: security.NewIPFilter(cfg.AllowedIPs, cfg.BlacklistIPs),
	}
}

func (h *HTTPListener) Name() string {
	if h.cfg.Name != "" {
		return h.cfg.Name
	}
	proto := "http"
	if h.cfg.Proto == "https" || h.cfg.TLSCert != "" {
		proto = "https"
	}
	return fmt.Sprintf("%s-%s:%d", proto, h.cfg.Bind, h.cfg.Port)
}

func (h *HTTPListener) Start(ctx context.Context, msgChan chan<- *kafka.Message) error {
	h.ctx, h.cancel = context.WithCancel(ctx)

	bindAddr := fmt.Sprintf("%s:%d", h.cfg.Bind, h.cfg.Port)
	if h.cfg.Bind == "" {
		bindAddr = fmt.Sprintf(":%d", h.cfg.Port)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		h.handleRequest(w, r, msgChan)
	})

	h.server = &http.Server{
		Addr:         bindAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		var err error
		if h.cfg.Proto == "https" || (h.cfg.TLSCert != "" && h.cfg.TLSKey != "") {
			log.Printf("[%s] Starting HTTPS listener on %s", h.Name(), bindAddr)
			err = h.server.ListenAndServeTLS(h.cfg.TLSCert, h.cfg.TLSKey)
		} else {
			log.Printf("[%s] Starting HTTP listener on %s", h.Name(), bindAddr)
			err = h.server.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			stats.IncListenerError(h.Name())
			log.Printf("[%s] HTTP server error: %v", h.Name(), err)
		}
	}()

	return nil
}

func (h *HTTPListener) handleRequest(w http.ResponseWriter, r *http.Request, msgChan chan<- *kafka.Message) {
	srcAddr := r.RemoteAddr
	if host, _, err := net.SplitHostPort(srcAddr); err == nil {
		srcAddr = host
	}

	if !h.ipFilter.IsAllowed(srcAddr) {
		stats.IncListenerError(h.Name())
		http.Error(w, "Forbidden: IP not allowed", http.StatusForbidden)
		return
	}

	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		stats.IncListenerError(h.Name())
		http.Error(w, "Only POST and PUT methods are allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, int64(h.cfg.MaxMsgSize)))
	if err != nil {
		stats.IncListenerError(h.Name())
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(body) == 0 {
		stats.IncListenerError(h.Name())
		http.Error(w, "Empty body", http.StatusBadRequest)
		return
	}

	// Decompress gzip / zlib / deflate payloads (via Content-Encoding header or magic bytes)
	contentEncoding := strings.ToLower(r.Header.Get("Content-Encoding"))
	isGzip := contentEncoding == "gzip" || (len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b)
	isDeflate := contentEncoding == "deflate" || contentEncoding == "zlib" || (len(body) >= 2 && body[0] == 0x78)

	var decompressed []byte
	if isGzip {
		if gzReader, err := gzip.NewReader(bytes.NewReader(body)); err == nil {
			decompressed, _ = io.ReadAll(gzReader)
			_ = gzReader.Close()
		}
	} else if isDeflate {
		if zReader, err := zlib.NewReader(bytes.NewReader(body)); err == nil {
			decompressed, _ = io.ReadAll(zReader)
			_ = zReader.Close()
		} else {
			// Fallback to raw RFC 1951 deflate
			fReader := flate.NewReader(bytes.NewReader(body))
			decompressed, _ = io.ReadAll(fReader)
			_ = fReader.Close()
		}
	}

	if len(decompressed) > 0 {
		body = decompressed
	}

	stats.IncMessagesReceived(h.Name(), len(body))

	// Extract dynamic topic and sensor_uuid from URL path
	// e.g. /rbdata/bd2fe2c3-d0ff-4c27-aef9-122b698239b9/rb_flow or /rb_flow
	urlPath := strings.Trim(r.URL.Path, "/")
	pathParts := strings.Split(urlPath, "/")

	pathTopic := ""
	pathSensorUUID := ""

	if len(pathParts) >= 3 && pathParts[0] == "rbdata" {
		pathSensorUUID = pathParts[1]
		pathTopic = pathParts[2]
	} else if len(pathParts) == 1 && pathParts[0] != "" && pathParts[0] != "rb_http2k" {
		pathTopic = pathParts[0]
	} else if len(pathParts) == 2 && pathParts[0] == "rbdata" {
		pathSensorUUID = pathParts[1]
	}

	bodyTrimmed := bytes.TrimSpace(body)
	if len(bodyTrimmed) > 0 {
		records := splitConcatenatedJSON(bodyTrimmed)
		if len(records) > 0 {
			count := 0
			for _, rec := range records {
				recBytes := rec
				if pathSensorUUID != "" && len(rec) > 0 && rec[0] == '{' {
					var itemObj map[string]interface{}
					if err := json.Unmarshal(rec, &itemObj); err == nil {
						if _, exists := itemObj["sensor_uuid"]; !exists {
							itemObj["sensor_uuid"] = pathSensorUUID
						}
						if _, exists := itemObj["proxy_uuid"]; !exists {
							itemObj["proxy_uuid"] = pathSensorUUID
						}
						recBytes, _ = json.Marshal(itemObj)
					}
				}

				formatted, targetTopic, err := h.decoder.FormatPayloadWithConfigAndTopic(recBytes, h.cfg, h.cfg.Proto, srcAddr)
				if err == nil && len(formatted) > 0 {
					topic := targetTopic
					if topic == "" {
						topic = pathTopic
					}
					if topic == "" {
						topic = h.cfg.Topic
					}
					msgChan <- &kafka.Message{
						Topic:     topic,
						Value:     formatted,
						Timestamp: time.Now(),
					}
					count++
				}
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"status":"ok","received":` + fmt.Sprintf("%d", count) + `}`))
			return
		}
	}

	maxScannerBuf := 10 * 1024 * 1024
	scanner := bufio.NewScanner(bytes.NewReader(bodyTrimmed))
	buf := make([]byte, 0, 65536)
	scanner.Buffer(buf, maxScannerBuf)
	count := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		lineTrimmed := bytes.TrimSpace(line)
		if len(lineTrimmed) == 0 {
			continue
		}

		lineBytes := lineTrimmed

		if pathSensorUUID != "" && len(lineTrimmed) > 0 && lineTrimmed[0] == '{' {
			var itemObj map[string]interface{}
			if err := json.Unmarshal(lineTrimmed, &itemObj); err == nil {
				if _, exists := itemObj["sensor_uuid"]; !exists {
					itemObj["sensor_uuid"] = pathSensorUUID
				}
				if _, exists := itemObj["proxy_uuid"]; !exists {
					itemObj["proxy_uuid"] = pathSensorUUID
				}
				lineBytes, _ = json.Marshal(itemObj)
			}
		}

		formatted, targetTopic, err := h.decoder.FormatPayloadWithConfigAndTopic(lineBytes, h.cfg, h.cfg.Proto, srcAddr)
		if err == nil && len(formatted) > 0 {
			topic := targetTopic
			if topic == "" {
				topic = pathTopic
			}
			if topic == "" {
				topic = h.cfg.Topic
			}
			msgChan <- &kafka.Message{
				Topic:     topic,
				Value:     formatted,
				Timestamp: time.Now(),
			}
			count++
		}
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"ok","received":` + fmt.Sprintf("%d", count) + `}`))
}

func (h *HTTPListener) Stop() error {
	if h.cancel != nil {
		h.cancel()
	}
	if h.server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.server.Shutdown(shutdownCtx)
	}
	h.wg.Wait()
	return nil
}

// splitConcatenatedJSON parses concatenated JSON objects (e.g. {...}{...}) or JSON arrays [...] without line breaks.
func splitConcatenatedJSON(data []byte) [][]byte {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil
	}

	if data[0] == '[' {
		var rawList []json.RawMessage
		if err := json.Unmarshal(data, &rawList); err == nil && len(rawList) > 0 {
			res := make([][]byte, len(rawList))
			for i, r := range rawList {
				res[i] = r
			}
			return res
		}
	}

	var records [][]byte
	depth := 0
	start := -1
	inString := false
	escaped := false

	for i := 0; i < len(data); i++ {
		ch := data[i]

		if inString {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				inString = false
			}
			continue
		}

		if ch == '"' {
			inString = true
			continue
		}

		if ch == '{' {
			if depth == 0 {
				start = i
			}
			depth++
		} else if ch == '}' {
			depth--
			if depth == 0 && start != -1 {
				records = append(records, data[start:i+1])
				start = -1
			}
		}
	}

	return records
}
