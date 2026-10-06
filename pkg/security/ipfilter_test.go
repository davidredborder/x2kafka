package security

import (
	"testing"
)

func TestIPFilter(t *testing.T) {
	allowed := []string{"10.1.33.0/24", "192.168.10.1"}
	blocked := []string{"10.1.33.100"}

	f := NewIPFilter(allowed, blocked)

	// Test 1: Allowed CIDR IP
	if !f.IsAllowed("10.1.33.5") {
		t.Errorf("Expected 10.1.33.5 to be allowed")
	}

	// Test 2: Blacklisted IP inside allowed subnet
	if f.IsAllowed("10.1.33.100") {
		t.Errorf("Expected 10.1.33.100 to be blocked by blacklist")
	}

	// Test 3: Unallowed IP
	if f.IsAllowed("172.16.0.1") {
		t.Errorf("Expected 172.16.0.1 to be rejected by allowlist")
	}

	// Test 4: Allowed specific IP
	if !f.IsAllowed("192.168.10.1:51423") {
		t.Errorf("Expected 192.168.10.1:51423 to be allowed")
	}
}
