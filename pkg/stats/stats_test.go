package stats

import (
	"testing"
)

func TestStatsCounters(t *testing.T) {
	RegisterListener("test_unix", "unix")
	IncMessagesReceived("test_unix", 128)
	IncMessagesSentKafka(128)

	st := GetStats()
	if st.MessagesReceived < 1 {
		t.Errorf("Expected MessagesReceived >= 1, got %d", st.MessagesReceived)
	}
	if st.MessagesSentKafka < 1 {
		t.Errorf("Expected MessagesSentKafka >= 1, got %d", st.MessagesSentKafka)
	}
	if len(st.Listeners) == 0 {
		t.Fatalf("Expected listeners in stats output")
	}
}
