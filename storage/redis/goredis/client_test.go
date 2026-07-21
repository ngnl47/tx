package goredis

import (
	"context"
	"strings"
	"testing"
)

func TestClientReturnsErrorWhenRedisClientMissing(t *testing.T) {
	client := NewClient(nil)

	if _, _, err := client.Get(context.Background(), "role:{1001}"); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("Get error = %v, want missing client error", err)
	}
	if err := client.Set(context.Background(), "role:{1001}", "{}"); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("Set error = %v, want missing client error", err)
	}
	if _, err := client.Eval(context.Background(), "return 'ok'", nil); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("Eval error = %v, want missing client error", err)
	}
	if _, err := client.MGet(context.Background(), "role:{1001}"); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("MGet error = %v, want missing client error", err)
	}
}
