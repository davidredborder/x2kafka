package listener

import (
	"testing"
)

func TestParseSyslog(t *testing.T) {
	raw3164 := "<34>Oct 11 22:14:15 mymachine su[1234]: 'su root' failed for lonvick on /dev/pts/8"
	msg := parseSyslog(raw3164, "syslog_udp", "127.0.0.1", nil)

	if msg.Pri != 34 {
		t.Errorf("Expected priority 34, got %d", msg.Pri)
	}
	if msg.SyslogFacility != 4 {
		t.Errorf("Expected facility 4, got %d", msg.SyslogFacility)
	}
	if msg.SyslogSeverity != 2 {
		t.Errorf("Expected severity 2, got %d", msg.SyslogSeverity)
	}
	if msg.Hostname != "mymachine" {
		t.Errorf("Expected hostname 'mymachine', got '%s'", msg.Hostname)
	}
	if msg.AppName != "su" {
		t.Errorf("Expected app_name 'su', got '%s'", msg.AppName)
	}
	if msg.ProcID != "1234" {
		t.Errorf("Expected proc_id '1234', got '%s'", msg.ProcID)
	}

	raw5424 := "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut=\"3\"] An application event log entry..."
	msg5424 := parseSyslog(raw5424, "syslog_udp", "127.0.0.1", nil)

	if msg5424.Pri != 165 {
		t.Errorf("Expected priority 165, got %d", msg5424.Pri)
	}
	if msg5424.ProtocolVersion != "1" {
		t.Errorf("Expected version '1', got '%s'", msg5424.ProtocolVersion)
	}
	if msg5424.Hostname != "mymachine.example.com" {
		t.Errorf("Expected hostname 'mymachine.example.com', got '%s'", msg5424.Hostname)
	}
}
