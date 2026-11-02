package sessionid

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestNewEtcdAllocatorRejectsMissingWriterOrKey(t *testing.T) {
	writer := revisionWriterFunc(func(context.Context, string, []byte, int64) (int64, error) {
		return 1, nil
	})
	for _, test := range []struct {
		name   string
		writer RevisionWriter
		key    string
	}{
		{name: "writer", key: "/session-ids/issuer"},
		{name: "key", writer: writer},
	} {
		t.Run(test.name, func(t *testing.T) {
			if allocator, err := NewEtcdAllocator(test.writer, test.key); err == nil || allocator != nil {
				t.Fatalf("NewEtcdAllocator = %v, %v, want nil and error", allocator, err)
			}
		})
	}
}

func TestEtcdAllocatorWritesOncePerAllocation(t *testing.T) {
	ctx := context.Background()
	const key = "/session-ids/issuer"
	var calls int
	var firstValue string
	allocator, err := NewEtcdAllocator(revisionWriterFunc(func(writeCtx context.Context, writeKey string, value []byte, leaseID int64) (int64, error) {
		calls++
		if writeCtx != ctx || writeKey != key || leaseID != 0 {
			t.Fatalf("write arguments = %v, %q, %d", writeCtx, writeKey, leaseID)
		}
		if calls == 1 {
			firstValue = string(value)
		}
		if len(value) == 0 || string(value) != firstValue {
			t.Fatalf("write value = %q, want fixed nonempty value %q", value, firstValue)
		}
		return int64(100 + calls), nil
	}), key)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []int64{101, 102} {
		id, err := allocator.AllocateSessionID(ctx)
		if err != nil || id != want {
			t.Fatalf("AllocateSessionID = %d, %v, want %d, nil", id, err, want)
		}
	}
	if calls != 2 {
		t.Fatalf("writes = %d, want 2", calls)
	}
}

func TestEtcdAllocatorChecksRevisionRange(t *testing.T) {
	for _, revision := range []int64{-1, 0, 1, MaxSessionID, MaxSessionID + 1} {
		t.Run(fmt.Sprint(revision), func(t *testing.T) {
			allocator, err := NewEtcdAllocator(revisionWriterFunc(func(context.Context, string, []byte, int64) (int64, error) {
				return revision, nil
			}), "/session-ids/issuer")
			if err != nil {
				t.Fatal(err)
			}
			id, err := allocator.AllocateSessionID(context.Background())
			if revision > 0 && revision <= MaxSessionID {
				if err != nil || id != revision {
					t.Fatalf("AllocateSessionID = %d, %v, want %d, nil", id, err, revision)
				}
			} else if err == nil || id != 0 {
				t.Fatalf("AllocateSessionID = %d, %v, want 0 and error", id, err)
			}
		})
	}
}

func TestEtcdAllocatorReturnsNoIDOnWriteError(t *testing.T) {
	wantErr := errors.New("write result unknown")
	calls := 0
	allocator, err := NewEtcdAllocator(revisionWriterFunc(func(context.Context, string, []byte, int64) (int64, error) {
		calls++
		return 42, wantErr
	}), "/session-ids/issuer")
	if err != nil {
		t.Fatal(err)
	}
	if id, err := allocator.AllocateSessionID(context.Background()); id != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("AllocateSessionID = %d, %v, want 0, %v", id, err, wantErr)
	}
	if calls != 1 {
		t.Fatalf("writes = %d, want 1 without retry", calls)
	}
}

func TestEtcdAllocatorReturnsNoIDWhenCanceled(t *testing.T) {
	for _, cancelBeforeWrite := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelBeforeWrite), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			allocator, err := NewEtcdAllocator(revisionWriterFunc(func(context.Context, string, []byte, int64) (int64, error) {
				calls++
				cancel()
				return 42, nil
			}), "/session-ids/issuer")
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if cancelBeforeWrite {
				cancel()
				wantCalls = 0
			}
			if id, err := allocator.AllocateSessionID(ctx); id != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("AllocateSessionID = %d, %v, want 0, context.Canceled", id, err)
			}
			if calls != wantCalls {
				t.Fatalf("writes = %d, want %d", calls, wantCalls)
			}
		})
	}
}

type revisionWriterFunc func(context.Context, string, []byte, int64) (int64, error)

func (writer revisionWriterFunc) PutRevision(ctx context.Context, key string, value []byte, leaseID int64) (int64, error) {
	return writer(ctx, key, value, leaseID)
}
