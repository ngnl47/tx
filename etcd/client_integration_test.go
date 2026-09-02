package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestClientCreateCompareAndSwapAndWatch(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	client := newIntegrationClient(t, endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := uniqueIntegrationKey(t, "client") + "/"
	key := prefix + "value"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.raw.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	input := []byte("v1")
	created, ok, err := client.Create(ctx, key, input, 0)
	if err != nil || !ok || created.ModRevision == 0 {
		t.Fatalf("create = %#v, %t, %v", created, ok, err)
	}
	input[0] = 'x'
	if !bytes.Equal(created.Value, []byte("v1")) {
		t.Fatalf("created value = %q, want v1", created.Value)
	}
	if _, ok, err := client.Create(ctx, key, []byte("duplicate"), 0); err != nil || ok {
		t.Fatalf("duplicate create = %t, %v, want false, nil", ok, err)
	}

	stored, ok, err := client.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("get = %#v, %t, %v", stored, ok, err)
	}
	stored.Value[0] = 'x'
	storedAgain, ok, err := client.Get(ctx, key)
	if err != nil || !ok || !bytes.Equal(storedAgain.Value, []byte("v1")) {
		t.Fatalf("get after mutation = %#v, %t, %v", storedAgain, ok, err)
	}

	snapshot, err := client.Snapshot(ctx, prefix)
	if err != nil || snapshot.Revision < created.ModRevision || len(snapshot.Values) != 1 {
		t.Fatalf("snapshot = %#v, %v", snapshot, err)
	}
	snapshot.Values[0].Value[0] = 'x'
	storedAgain, ok, err = client.Get(ctx, key)
	if err != nil || !ok || !bytes.Equal(storedAgain.Value, []byte("v1")) {
		t.Fatalf("get after snapshot mutation = %#v, %t, %v", storedAgain, ok, err)
	}

	watchCtx, cancelWatch := context.WithCancel(ctx)
	t.Cleanup(cancelWatch)
	responses := client.WatchPrefix(watchCtx, prefix, created.ModRevision+1)
	if ok, err := client.CompareAndSwap(ctx, key, created.ModRevision, []byte("v2")); err != nil || !ok {
		t.Fatalf("compare and swap = %t, %v", ok, err)
	}
	putResponse := waitWatchResponse(t, responses)
	if putResponse.Err != nil || len(putResponse.Events) != 1 || putResponse.Events[0].Type != EventPut ||
		putResponse.Events[0].Value.Key != key || !bytes.Equal(putResponse.Events[0].Value.Value, []byte("v2")) {
		t.Fatalf("put watch response = %#v", putResponse)
	}
	if ok, err := client.CompareAndSwap(ctx, key, created.ModRevision, []byte("stale")); err != nil || ok {
		t.Fatalf("stale compare and swap = %t, %v, want false, nil", ok, err)
	}
	if err := client.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	deleteResponse := waitWatchResponse(t, responses)
	if deleteResponse.Err != nil || len(deleteResponse.Events) != 1 || deleteResponse.Events[0].Type != EventDelete ||
		deleteResponse.Events[0].Value.Key != key {
		t.Fatalf("delete watch response = %#v", deleteResponse)
	}
	if _, ok, err := client.Get(ctx, key); err != nil || ok {
		t.Fatalf("get deleted key = %t, %v, want false, nil", ok, err)
	}

	cancelWatch()
	select {
	case _, ok := <-responses:
		if ok {
			t.Fatal("watch channel remained open after cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch channel did not close after cancellation")
	}

	if err := client.Put(ctx, key, []byte("v3"), 0); err != nil {
		t.Fatal(err)
	}
	stored, ok, err = client.Get(ctx, key)
	if err != nil || !ok || !bytes.Equal(stored.Value, []byte("v3")) {
		t.Fatalf("get put value = %#v, %t, %v", stored, ok, err)
	}
}

func TestClientProbe(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	client := newIntegrationClient(t, endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Probe(ctx); err != nil {
		t.Fatal(err)
	}

	canceledCtx, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	if err := client.Probe(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("probe error = %v, want context.Canceled", err)
	}
}

func TestClientCompareAndSwapPreservesLease(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	client := newIntegrationClient(t, endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lease, err := client.raw.Grant(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.raw.Revoke(cleanupCtx, lease.ID)
	})

	key := uniqueIntegrationKey(t, "lease")
	created, ok, err := client.Create(ctx, key, []byte("v1"), int64(lease.ID))
	if err != nil || !ok {
		t.Fatalf("create = %#v, %t, %v", created, ok, err)
	}
	if ok, err := client.CompareAndSwap(ctx, key, created.ModRevision, []byte("v2")); err != nil || !ok {
		t.Fatalf("compare and swap = %t, %v", ok, err)
	}
	stored, ok, err := client.Get(ctx, key)
	if err != nil || !ok || stored.LeaseID != int64(lease.ID) {
		t.Fatalf("get = %#v, %t, %v, want lease %d", stored, ok, err, lease.ID)
	}
}

func newIntegrationClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, Config{Endpoints: []string{endpoint}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close etcd client: %v", err)
		}
	})
	return client
}

func uniqueIntegrationKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("/tx/tests/etcd/%s/%d", suffix, time.Now().UnixNano())
}

func waitWatchResponse(t *testing.T, responses <-chan WatchResponse) WatchResponse {
	t.Helper()
	select {
	case response, ok := <-responses:
		if !ok {
			t.Fatal("watch channel closed before the expected response")
		}
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch response")
		return WatchResponse{}
	}
}
