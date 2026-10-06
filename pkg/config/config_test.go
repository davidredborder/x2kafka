package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	content := `{
		"brokers": ["127.0.0.1:9092"],
		"topic": "test_topic",
		"listeners": [
			{
				"name": "test_fifo",
				"proto": "fifo",
				"path": "/tmp/test.fifo"
			}
		]
	}`

	tmpfile, err := os.CreateTemp("", "config_test_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	if cfg.Topic != "test_topic" {
		t.Errorf("Expected topic 'test_topic', got '%s'", cfg.Topic)
	}

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "127.0.0.1:9092" {
		t.Errorf("Expected brokers ['127.0.0.1:9092'], got %v", cfg.Brokers)
	}

	if len(cfg.Listeners) != 1 {
		t.Fatalf("Expected 1 listener, got %d", len(cfg.Listeners))
	}

	if cfg.Listeners[0].Name != "test_fifo" {
		t.Errorf("Expected listener name 'test_fifo', got '%s'", cfg.Listeners[0].Name)
	}
}
