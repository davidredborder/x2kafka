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

func TestLoadEgressConfig(t *testing.T) {
	for _, filename := range []string{"../../configs_example/kafka2x_config.json", "../../configs_example/kafka2x_config.yaml"} {
		cfg, err := LoadEgressConfig(filename)
		if err != nil {
			t.Fatalf("LoadEgressConfig(%s) returned error: %v", filename, err)
		}
		if cfg.HTTP.URL == "" {
			t.Errorf("Expected non-empty HTTP URL for %s", filename)
		}
	}

	userConfig := `{
  "debug": true,
  "kafka": {
    "brokers": ["kafka.service:9092"],
    "group_id": "kafka2x-http-forwarder",
    "topics": [
      "rb_flow",
      "rb_monitor",
      "rb_syslog",
      "rb_trap"
    ]
  },
  "pipeline": {
    "workers": 2,
    "batch_size": 1000,
    "timeout_ms": 100,
    "retries": 3,
    "backoff_sec": 2
  },
  "http": {
    "url": "https://http2k.redborder.cluster/rbdata/938c7905-807e-4b8f-b488-70d525ac933b",
    "method": "POST",
    "framing": "newline",
    "compression": "gzip",
    "insecure": false,
    "timeout": "15s"
  },
  "stats_socket": "/run/x2kafka/kafka2x.sock"
}`

	tmpfile, err := os.CreateTemp("", "user_kafka2x_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())
	_, _ = tmpfile.Write([]byte(userConfig))
	tmpfile.Close()

	cfg, err := LoadEgressConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to parse user config: %v", err)
	}
	if !cfg.Debug {
		t.Errorf("Expected debug=true")
	}
	if cfg.HTTP.Timeout != "15s" {
		t.Errorf("Expected timeout '15s', got %q", cfg.HTTP.Timeout)
	}
}

