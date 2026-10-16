package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
	"x2kafka/pkg/egress"
	"x2kafka/pkg/kafka"
)

// EgressConfig defines the complete configuration for kafka2x.
type EgressConfig struct {
	Debug       bool                  `json:"debug" yaml:"debug"`
	Kafka       kafka.ConsumerConfig  `json:"kafka" yaml:"kafka"`
	Pipeline    egress.PipelineConfig `json:"pipeline" yaml:"pipeline"`
	HTTP        egress.HTTPConfig     `json:"http" yaml:"http"`
	StatsPort   int                   `json:"stats_port,omitempty" yaml:"stats_port,omitempty"`
	StatsBind   string                `json:"stats_bind,omitempty" yaml:"stats_bind,omitempty"`
	StatsSocket string                `json:"stats_socket,omitempty" yaml:"stats_socket,omitempty"`
}

// LoadEgressConfig loads a kafka2x configuration file from JSON or YAML.
func LoadEgressConfig(filename string) (*EgressConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read egress config file %s: %w", filename, err)
	}

	cfg := &EgressConfig{
		Pipeline: egress.PipelineConfig{
			Workers:       2,
			BatchSize:     500,
			TimeoutMillis: 100,
			Retries:       3,
			BackoffSec:    2,
		},
		HTTP: egress.HTTPConfig{
			Method:      "POST",
			Framing:     "newline",
			Compression: "none",
		},
		StatsPort:   7989,
		StatsSocket: "/run/kafka2x/kafka2x.sock",
	}

	isYAML := strings.HasSuffix(filename, ".yml") || strings.HasSuffix(filename, ".yaml")
	if isYAML {
		// First try mapping legacy k2http format if present
		var rawMap map[string]interface{}
		if err := yaml.Unmarshal(data, &rawMap); err == nil {
			parseLegacyK2HTTPYaml(rawMap, cfg)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse yaml config: %w", err)
		}
	} else {
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse json config: %w", err)
		}
	}

	return cfg, nil
}

func parseLegacyK2HTTPYaml(raw map[string]interface{}, cfg *EgressConfig) {
	if kMap, ok := raw["kafka"].(map[string]interface{}); ok {
		if b, ok := kMap["broker"].(string); ok {
			cfg.Kafka.Brokers = []string{b}
		}
		if cg, ok := kMap["consumergroup"].(string); ok {
			cfg.Kafka.GroupID = cg
		}
		if topics, ok := kMap["topics"].([]interface{}); ok {
			for _, t := range topics {
				if tStr, ok := t.(string); ok {
					cfg.Kafka.Topics = append(cfg.Kafka.Topics, tStr)
				}
			}
		}
	}

	if bMap, ok := raw["batch"].(map[string]interface{}); ok {
		if w, ok := bMap["workers"].(int); ok {
			cfg.Pipeline.Workers = w
		}
		if s, ok := bMap["size"].(int); ok {
			cfg.Pipeline.BatchSize = s
		}
		if tm, ok := bMap["timeoutMillis"].(int); ok {
			cfg.Pipeline.TimeoutMillis = tm
		}
		if def, ok := bMap["deflate"].(bool); ok && def {
			cfg.HTTP.Compression = "deflate"
		}
	}

	if hMap, ok := raw["http"].(map[string]interface{}); ok {
		if u, ok := hMap["url"].(string); ok {
			cfg.HTTP.URL = u
		}
		if insec, ok := hMap["insecure"].(bool); ok {
			cfg.HTTP.Insecure = insec
		}
	}

	if pMap, ok := raw["pipeline"].(map[string]interface{}); ok {
		if r, ok := pMap["retries"].(int); ok {
			cfg.Pipeline.Retries = r
		}
		if b, ok := pMap["backoff"].(int); ok {
			cfg.Pipeline.BackoffSec = b
		}
	}
}
