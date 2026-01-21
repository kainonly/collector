package transfer

import (
	"testing"
)

func TestStreamName(t *testing.T) {
	x := &Transfer{Namespace: "test"}

	tests := []struct {
		key  string
		want string
	}{
		{"metrics", "test_metrics"},
		{"events", "test_events"},
		{"logs", "test_logs"},
	}

	for _, tt := range tests {
		if got := x.StreamName(tt.key); got != tt.want {
			t.Errorf("StreamName(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestSubName(t *testing.T) {
	x := &Transfer{Namespace: "test"}

	tests := []struct {
		key  string
		want string
	}{
		{"metrics", "test.metrics"},
		{"events", "test.events"},
		{"logs", "test.logs"},
	}

	for _, tt := range tests {
		if got := x.SubName(tt.key); got != tt.want {
			t.Errorf("SubName(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}
