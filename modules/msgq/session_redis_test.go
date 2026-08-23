package msgq

import (
	"context"
	"errors"
	"testing"
	"time"
)

type recordingSessionRedisClient struct {
	evalCalls int
}

func (client *recordingSessionRedisClient) Get(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (client *recordingSessionRedisClient) Eval(context.Context, string, []string, ...any) (string, error) {
	client.evalCalls++
	return "", nil
}

func TestRedisSessionStoreRejectsSubMillisecondTTLBeforeIO(t *testing.T) {
	client := &recordingSessionRedisClient{}
	store, err := NewRedisSessionStore(client, SessionRedisOptions{Prefix: "tx:test", TTL: time.Nanosecond})
	if !errors.Is(err, ErrInvalidSessionOptions) {
		t.Fatalf("NewRedisSessionStore error = %v, want ErrInvalidSessionOptions", err)
	}
	if store != nil {
		t.Fatalf("NewRedisSessionStore = %#v, want nil", store)
	}
	if client.evalCalls != 0 {
		t.Fatalf("eval calls = %d, want no Redis I/O", client.evalCalls)
	}
}
