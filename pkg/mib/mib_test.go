package mib

import (
	"testing"
)

func TestResolveOIDFallback(t *testing.T) {
	// Without MIBs loaded, returns original OID
	res := ResolveOID(".1.3.6.1.4.1.9.9.43.2.0.1")
	if res != ".1.3.6.1.4.1.9.9.43.2.0.1" {
		t.Errorf("Expected fallback to '.1.3.6.1.4.1.9.9.43.2.0.1', got '%s'", res)
	}
}
