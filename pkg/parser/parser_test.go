package parser

import (
	"testing"
)

func TestParseKV(t *testing.T) {
	input := `srcip=192.168.1.50 dstip=10.0.0.1 dstport=443 action=deny reason="policy violation" allow=true ratio=4.5`
	res := ParseKV(input)

	if res["srcip"] != "192.168.1.50" {
		t.Errorf("expected srcip=192.168.1.50, got %v", res["srcip"])
	}
	if res["dstip"] != "10.0.0.1" {
		t.Errorf("expected dstip=10.0.0.1, got %v", res["dstip"])
	}
	if res["dstport"] != int64(443) {
		t.Errorf("expected dstport=443 (int64), got %v (%T)", res["dstport"], res["dstport"])
	}
	if res["action"] != "deny" {
		t.Errorf("expected action=deny, got %v", res["action"])
	}
	if res["reason"] != "policy violation" {
		t.Errorf("expected reason='policy violation', got %v", res["reason"])
	}
	if res["allow"] != true {
		t.Errorf("expected allow=true, got %v", res["allow"])
	}
	if res["ratio"] != 4.5 {
		t.Errorf("expected ratio=4.5, got %v", res["ratio"])
	}
}

func TestParseCEF(t *testing.T) {
	input := `CEF:0|Security|Firewall|1.0|100|Connection Denied|5|src=10.0.0.50 dst=192.168.1.1 spt=12345 dpt=80 proto=TCP`
	res, ok := ParseCEF(input)
	if !ok {
		t.Fatalf("expected CEF parse success")
	}

	if res["cef_vendor"] != "Security" {
		t.Errorf("expected cef_vendor=Security, got %v", res["cef_vendor"])
	}
	if res["cef_severity"] != int64(5) {
		t.Errorf("expected cef_severity=5, got %v", res["cef_severity"])
	}
	if res["src"] != "10.0.0.50" {
		t.Errorf("expected src=10.0.0.50, got %v", res["src"])
	}
	if res["dpt"] != int64(80) {
		t.Errorf("expected dpt=80, got %v", res["dpt"])
	}
}

func TestParseRegex(t *testing.T) {
	patterns := []string{
		`Failed password for (invalid user )?(?P<username>\S+) from (?P<src_ip>\S+) port (?P<src_port>\d+) ssh2`,
	}
	p, err := NewParser(patterns)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	input := `Failed password for invalid user admin from 192.168.10.50 port 54321 ssh2`
	res := p.ParseRegex(input)

	if res["username"] != "admin" {
		t.Errorf("expected username=admin, got %v", res["username"])
	}
	if res["src_ip"] != "192.168.10.50" {
		t.Errorf("expected src_ip=192.168.10.50, got %v", res["src_ip"])
	}
	if res["src_port"] != int64(54321) {
		t.Errorf("expected src_port=54321, got %v", res["src_port"])
	}
}

func TestNormalizeSchema(t *testing.T) {
	rawFields := map[string]interface{}{
		"src":      "192.168.1.1",
		"dst":      "10.0.0.2",
		"spt":      int64(1234),
		"dpt":      int64(80),
		"proto":    "TCP",
		"user":     "alice",
		"custom_k": "custom_v",
	}

	norm := NormalizeSchema(rawFields)

	if norm["src_ip"] != "192.168.1.1" {
		t.Errorf("expected src_ip, got %v", norm["src_ip"])
	}
	if norm["dst_ip"] != "10.0.0.2" {
		t.Errorf("expected dst_ip, got %v", norm["dst_ip"])
	}
	if norm["src_port"] != int64(1234) {
		t.Errorf("expected src_port, got %v", norm["src_port"])
	}
	if norm["dst_port"] != int64(80) {
		t.Errorf("expected dst_port, got %v", norm["dst_port"])
	}
	if norm["transport"] != "TCP" {
		t.Errorf("expected transport, got %v", norm["transport"])
	}
	if norm["username"] != "alice" {
		t.Errorf("expected username, got %v", norm["username"])
	}
	if norm["custom_k"] != "custom_v" {
		t.Errorf("expected custom_k, got %v", norm["custom_k"])
	}
}
