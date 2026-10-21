package flow

import (
	"testing"
)

func TestHomeNetOrientation(t *testing.T) {
	checker, err := NewHomeNetChecker([]string{"10.0.0.0/8", "192.168.0.0/16"})
	if err != nil {
		t.Fatalf("failed to create HomeNetChecker: %v", err)
	}

	// 1. Upstream: local client (10.1.1.5:45678) -> external server (1.1.1.1:443)
	upRec := &JSONFlowRecord{
		LanIP:            "10.1.1.5",
		WanIP:            "1.1.1.1",
		LanL4Port:        45678,
		WanL4Port:        443,
		ClientMAC:        "aa:aa:aa:aa:aa:aa",
		ServerMAC:        "bb:bb:bb:bb:bb:bb",
		LanInterface:     10,
		LanInterfaceName: "10",
		WanInterface:     20,
		WanInterfaceName: "20",
	}
	checker.OrientFlow(upRec)

	if upRec.Direction != "upstream" {
		t.Errorf("expected direction 'upstream', got '%s'", upRec.Direction)
	}
	if upRec.LanIP != "10.1.1.5" || upRec.WanIP != "1.1.1.1" {
		t.Errorf("expected LanIP 10.1.1.5 and WanIP 1.1.1.1, got %s and %s", upRec.LanIP, upRec.WanIP)
	}
	if upRec.LanL4Port != 45678 || upRec.WanL4Port != 443 {
		t.Errorf("expected ports 45678 -> 443, got %d -> %d", upRec.LanL4Port, upRec.WanL4Port)
	}

	// 2. Downstream: external server (1.1.1.1:443) -> local client (10.1.1.5:45678)
	// In RedBorder standard: LanIP is ALWAYS the local home_net IP (10.1.1.5), and WanIP is the remote IP (1.1.1.1)
	downRec := &JSONFlowRecord{
		LanIP:            "1.1.1.1",
		WanIP:            "10.1.1.5",
		LanL4Port:        443,
		WanL4Port:        45678,
		ClientMAC:        "bb:bb:bb:bb:bb:bb",
		ServerMAC:        "aa:aa:aa:aa:aa:aa",
		LanInterface:     20,
		LanInterfaceName: "20",
		WanInterface:     10,
		WanInterfaceName: "10",
	}
	checker.OrientFlow(downRec)

	if downRec.Direction != "downstream" {
		t.Errorf("expected direction 'downstream', got '%s'", downRec.Direction)
	}
	if downRec.LanIP != "10.1.1.5" || downRec.WanIP != "1.1.1.1" {
		t.Errorf("expected LanIP 10.1.1.5 and WanIP 1.1.1.1, got %s and %s", downRec.LanIP, downRec.WanIP)
	}
	if downRec.LanL4Port != 45678 || downRec.WanL4Port != 443 {
		t.Errorf("expected ports 45678 and 443, got %d and %d", downRec.LanL4Port, downRec.WanL4Port)
	}
	if downRec.ClientMAC != "aa:aa:aa:aa:aa:aa" || downRec.ServerMAC != "bb:bb:bb:bb:bb:bb" {
		t.Errorf("expected ClientMAC aa:aa:aa:aa:aa:aa and ServerMAC bb:bb:bb:bb:bb:bb, got %s and %s", downRec.ClientMAC, downRec.ServerMAC)
	}
	if downRec.LanInterface != 10 || downRec.WanInterface != 20 {
		t.Errorf("expected LanInterface 10 and WanInterface 20, got %d and %d", downRec.LanInterface, downRec.WanInterface)
	}

	// 3. Internal: local (10.1.1.5) -> local (192.168.1.100)
	intRec := &JSONFlowRecord{
		LanIP:     "10.1.1.5",
		WanIP:     "192.168.1.100",
		LanL4Port: 5000,
		WanL4Port: 80,
	}
	checker.OrientFlow(intRec)

	if intRec.Direction != "internal" {
		t.Errorf("expected direction 'internal', got '%s'", intRec.Direction)
	}
}

func TestDefaultHomeNets(t *testing.T) {
	// Empty config should default to RFC 1918
	checker, err := NewHomeNetChecker(nil)
	if err != nil {
		t.Fatalf("failed to create default HomeNetChecker: %v", err)
	}

	rec := &JSONFlowRecord{
		LanIP: "172.16.5.10",
		WanIP: "8.8.8.8",
	}
	checker.OrientFlow(rec)

	if rec.Direction != "upstream" {
		t.Errorf("expected default RFC 1918 to recognize 172.16.x.x as upstream, got '%s'", rec.Direction)
	}
}
