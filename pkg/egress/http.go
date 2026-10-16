package egress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// HTTPConfig defines settings for sending batches to an HTTP endpoint.
type HTTPConfig struct {
	URL          string            `json:"url" yaml:"url"`
	Method       string            `json:"method,omitempty" yaml:"method,omitempty"`
	Compression  string            `json:"compression,omitempty" yaml:"compression,omitempty"` // "none", "gzip", "zlib", "deflate"
	Framing      string            `json:"framing,omitempty" yaml:"framing,omitempty"`         // "newline" (\n separated), "concatenated" ({}{}), or "json_array" ([{},{}])
	Insecure     bool              `json:"insecure,omitempty" yaml:"insecure,omitempty"`
	Timeout      string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Headers      map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
	MaxIdleConns int               `json:"max_idle_conns,omitempty" yaml:"max_idle_conns,omitempty"`
}

// HTTPSender handles reliable, pooled HTTP POST batch delivery.
type HTTPSender struct {
	cfg    HTTPConfig
	client *http.Client
}

// NewHTTPSender creates an HTTPSender with tuned connection pooling.
func NewHTTPSender(cfg HTTPConfig) (*HTTPSender, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("HTTP destination URL is required")
	}
	if cfg.Method == "" {
		cfg.Method = "POST"
	}
	timeout := 15 * time.Second
	if cfg.Timeout != "" {
		parsed, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP timeout %q: %w", cfg.Timeout, err)
		}
		if parsed > 0 {
			timeout = parsed
		}
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 100
	}
	if cfg.Framing == "" {
		cfg.Framing = "newline"
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConns,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.Insecure,
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	return &HTTPSender{
		cfg:    cfg,
		client: client,
	}, nil
}

// SendBatch formats, optionally compresses, and sends a slice of raw payload bytes.
func (h *HTTPSender) SendBatch(ctx context.Context, records [][]byte) error {
	return h.SendBatchToTopic(ctx, "", records)
}

// SendBatchToTopic formats, optionally compresses, and sends a slice of raw payload bytes to the topic-qualified URL.
func (h *HTTPSender) SendBatchToTopic(ctx context.Context, topic string, records [][]byte) error {
	if len(records) == 0 {
		return nil
	}

	var framedPayload bytes.Buffer
	switch h.cfg.Framing {
	case "concatenated":
		// Matches rbforwarder / k2http style: {...}{...}
		for _, r := range records {
			trimmed := bytes.TrimSpace(r)
			if len(trimmed) > 0 {
				framedPayload.Write(trimmed)
			}
		}
	case "json_array":
		framedPayload.WriteByte('[')
		first := true
		for _, r := range records {
			trimmed := bytes.TrimSpace(r)
			if len(trimmed) > 0 {
				if !first {
					framedPayload.WriteByte(',')
				}
				framedPayload.Write(trimmed)
				first = false
			}
		}
		framedPayload.WriteByte(']')
	default:
		// "newline" delimited
		for _, r := range records {
			trimmed := bytes.TrimSpace(r)
			if len(trimmed) > 0 {
				framedPayload.Write(trimmed)
				framedPayload.WriteByte('\n')
			}
		}
	}

	rawBytes := framedPayload.Bytes()
	if len(rawBytes) == 0 {
		return nil
	}

	var bodyReader io.Reader
	var contentEncoding string

	switch strings.ToLower(h.cfg.Compression) {
	case "gzip":
		var gzBuf bytes.Buffer
		gzWriter := gzip.NewWriter(&gzBuf)
		if _, err := gzWriter.Write(rawBytes); err != nil {
			return fmt.Errorf("gzip write error: %w", err)
		}
		if err := gzWriter.Close(); err != nil { // Properly close with checksum footer
			return fmt.Errorf("gzip close error: %w", err)
		}
		bodyReader = &gzBuf
		contentEncoding = "gzip"

	case "zlib", "deflate":
		var zBuf bytes.Buffer
		// Clean zlib Writer with proper Close() ensuring valid Adler-32 checksum!
		zWriter := zlib.NewWriter(&zBuf)
		if _, err := zWriter.Write(rawBytes); err != nil {
			return fmt.Errorf("zlib write error: %w", err)
		}
		if err := zWriter.Close(); err != nil {
			return fmt.Errorf("zlib close error: %w", err)
		}
		bodyReader = &zBuf
		contentEncoding = "deflate"

	case "raw_deflate":
		var fBuf bytes.Buffer
		fWriter, err := flate.NewWriter(&fBuf, flate.DefaultCompression)
		if err != nil {
			return fmt.Errorf("flate writer error: %w", err)
		}
		if _, err := fWriter.Write(rawBytes); err != nil {
			return fmt.Errorf("flate write error: %w", err)
		}
		if err := fWriter.Close(); err != nil {
			return fmt.Errorf("flate close error: %w", err)
		}
		bodyReader = &fBuf
		contentEncoding = "deflate"

	default:
		bodyReader = bytes.NewReader(rawBytes)
	}

	targetURL := strings.TrimRight(h.cfg.URL, "/")
	if topic != "" && !strings.HasSuffix(targetURL, "/"+topic) {
		targetURL = targetURL + "/" + topic
	}

	req, err := http.NewRequestWithContext(ctx, h.cfg.Method, targetURL, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "kafka2x/1.0.0 (redBorder)")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	for k, v := range h.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("http POST to %s failed: %w", targetURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("http error %d from %s: %s", resp.StatusCode, targetURL, string(respBody))
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
