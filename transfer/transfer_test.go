package transfer

import (
	"testing"
)

// TestStreamName 测试流名称生成。
//
// 验证 StreamName 方法正确生成 {namespace}_{key} 格式的流名称。
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

// TestSubName 测试订阅主题名称生成。
//
// 验证 SubName 方法正确生成 {namespace}.{key} 格式的主题名称。
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
