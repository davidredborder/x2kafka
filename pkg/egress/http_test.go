package egress

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHTTPSenderFramingAndCompression(t *testing.T) {
	var receivedBody []byte
	var receivedEncoding string
	var reqCount int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		receivedEncoding = r.Header.Get("Content-Encoding")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Error reading request body: %v", err)
		}
		receivedBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	testRecords := [][]byte{
		[]byte(`{"msg":"event 1","sensor":"s1"}`),
		[]byte(`{"msg":"event 2","sensor":"s2"}`),
	}

	// 1. Test Newline framing with GZIP
	senderGzip, err := NewHTTPSender(HTTPConfig{
		URL:         ts.URL,
		Compression: "gzip",
		Framing:     "newline",
	})
	if err != nil {
		t.Fatalf("Failed to create HTTPSender: %v", err)
	}

	err = senderGzip.SendBatch(context.Background(), testRecords)
	if err != nil {
		t.Fatalf("SendBatch error: %v", err)
	}

	if receivedEncoding != "gzip" {
		t.Errorf("Expected Content-Encoding gzip, got %s", receivedEncoding)
	}

	// Decompress and verify
	gzReader, err := gzip.NewReader(bytes.NewReader(receivedBody))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed data: %v", err)
	}
	expectedNewline := "{\"msg\":\"event 1\",\"sensor\":\"s1\"}\n{\"msg\":\"event 2\",\"sensor\":\"s2\"}\n"
	if string(decompressed) != expectedNewline {
		t.Errorf("Expected %q, got %q", expectedNewline, string(decompressed))
	}

	// 2. Test Concatenated framing with ZLIB (verifies valid Adler-32 checksum footer!)
	senderZlib, err := NewHTTPSender(HTTPConfig{
		URL:         ts.URL,
		Compression: "deflate",
		Framing:     "concatenated",
	})
	if err != nil {
		t.Fatalf("Failed to create HTTPSender: %v", err)
	}

	err = senderZlib.SendBatch(context.Background(), testRecords)
	if err != nil {
		t.Fatalf("SendBatch error: %v", err)
	}

	if receivedEncoding != "deflate" {
		t.Errorf("Expected Content-Encoding deflate, got %s", receivedEncoding)
	}

	zReader, err := zlib.NewReader(bytes.NewReader(receivedBody))
	if err != nil {
		t.Fatalf("Failed to create zlib reader: %v", err)
	}
	decompressedZlib, err := io.ReadAll(zReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed zlib (Adler32 error?): %v", err)
	}
	expectedConcatenated := `{"msg":"event 1","sensor":"s1"}{"msg":"event 2","sensor":"s2"}`
	if string(decompressedZlib) != expectedConcatenated {
		t.Errorf("Expected %q, got %q", expectedConcatenated, string(decompressedZlib))
	}
}

func TestHTTPSenderRetryAndTimeout(t *testing.T) {
	failAttempts := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&failAttempts, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Temporary failure"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	sender, _ := NewHTTPSender(HTTPConfig{
		URL:     ts.URL,
		Timeout: "2s",
	})

	// First call should fail with 500
	err := sender.SendBatch(context.Background(), [][]byte{[]byte(`{"test":1}`)})
	if err == nil {
		t.Errorf("Expected error on 500 status, got nil")
	}

	// Second call should succeed with 200
	err = sender.SendBatch(context.Background(), [][]byte{[]byte(`{"test":2}`)})
	if err != nil {
		t.Errorf("Expected success on 2nd attempt, got: %v", err)
	}
}

func TestHTTPSenderTopicURLAppending(t *testing.T) {
	var lastPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath = r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	sender, err := NewHTTPSender(HTTPConfig{
		URL: ts.URL + "/rbdata/938c7905-807e-4b8f-b488-70d525ac933b",
	})
	if err != nil {
		t.Fatalf("Failed to create HTTPSender: %v", err)
	}

	err = sender.SendBatchToTopic(context.Background(), "rb_flow", [][]byte{[]byte(`{"flow":1}`)})
	if err != nil {
		t.Fatalf("SendBatchToTopic failed: %v", err)
	}

	expectedPath := "/rbdata/938c7905-807e-4b8f-b488-70d525ac933b/rb_flow"
	if lastPath != expectedPath {
		t.Errorf("Expected request path %q, got %q", expectedPath, lastPath)
	}
}

