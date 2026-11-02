package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestClientPutRevisionReturnsWriteRevision(t *testing.T) {
	client := integrationClientFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := "/tx/tests/etcd/put-revision/" + uuid.NewString()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := client.Delete(cleanupCtx, key); err != nil {
			t.Errorf("delete test key: %v", err)
		}
		if _, found, err := client.Get(cleanupCtx, key); err != nil || found {
			t.Errorf("test key after cleanup: found=%t, error=%v", found, err)
		}
	})
	lease, err := client.Grant(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := lease.Revoke(cleanupCtx); err != nil {
			t.Errorf("revoke test lease: %v", err)
		}
	})
	var previous int64
	for range 2 {
		revision, err := client.PutRevision(ctx, key, []byte("same value"), lease.ID())
		if err != nil || revision <= previous {
			t.Fatalf("PutRevision = %d, %v, want revision > %d", revision, err, previous)
		}
		stored, found, err := client.Get(ctx, key)
		if err != nil || !found || stored.ModRevision != revision || stored.LeaseID != lease.ID() {
			t.Fatalf("stored value = %#v, %t, %v, want revision %d and lease %d", stored, found, err, revision, lease.ID())
		}
		previous = revision
	}
	canceledCtx, cancelCanceled := context.WithCancel(ctx)
	cancelCanceled()
	if revision, err := client.PutRevision(canceledCtx, key, []byte("canceled"), 0); revision != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled PutRevision = %d, %v, want 0, context.Canceled", revision, err)
	}
}

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

func TestLeaseStopExpiresKeyWithoutImmediateRevoke(t *testing.T) {
	client := integrationClientFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, err := client.Grant(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = lease.Revoke(cleanupCtx)
	})

	key := uniqueIntegrationKey(t, "lease-stop")
	if err := lease.Put(ctx, key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := lease.Stop(); err != nil {
		t.Fatal(err)
	}
	assertKeyExists(t, client, key)
	assertKeyEventuallyMissing(t, client, key, 4*time.Second)
	<-lease.Done()
	if !errors.Is(lease.Err(), ErrLeaseStopped) {
		t.Fatalf("lease error = %v, want ErrLeaseStopped", lease.Err())
	}
}

func TestLeaseRevokeImmediatelyRemovesCreatedKey(t *testing.T) {
	client := integrationClientFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, err := client.Grant(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.ID() == 0 {
		t.Fatal("lease ID is zero")
	}

	key := uniqueIntegrationKey(t, "lease-revoke")
	created, ok, err := lease.Create(ctx, key, []byte("value"))
	if err != nil || !ok || created.LeaseID != lease.ID() {
		t.Fatalf("create = %#v, %t, %v", created, ok, err)
	}
	if _, ok, err := lease.Create(ctx, key, []byte("duplicate")); err != nil || ok {
		t.Fatalf("duplicate create = %t, %v, want false, nil", ok, err)
	}
	if err := lease.Revoke(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lease.Revoke(ctx); err != nil {
		t.Fatalf("second revoke: %v", err)
	}
	if _, ok, err := client.Get(ctx, key); err != nil || ok {
		t.Fatalf("get revoked key = %t, %v, want false, nil", ok, err)
	}
	<-lease.Done()
	if !errors.Is(lease.Err(), ErrLeaseStopped) {
		t.Fatalf("lease error = %v, want ErrLeaseStopped", lease.Err())
	}
}

func TestLeaseClientCloseReportsLeaseLost(t *testing.T) {
	client := integrationClientFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, err := client.Grant(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-lease.Done():
		if !errors.Is(lease.Err(), ErrLeaseLost) {
			t.Fatalf("lease error = %v, want ErrLeaseLost", lease.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lease did not report client closure")
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

func integrationClientFromEnv(t *testing.T) *Client {
	t.Helper()
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	return newIntegrationClient(t, endpoint)
}

func assertKeyExists(t *testing.T, client *Client, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, ok, err := client.Get(ctx, key); err != nil || !ok {
		t.Fatalf("get existing key = %t, %v", ok, err)
	}
}

func assertKeyEventuallyMissing(t *testing.T, client *Client, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, ok, err := client.Get(ctx, key)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("key %q still exists after %s", key, timeout)
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

func TestSnapshotCASRejectsDeletedRequiredMember(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ETCD_ENDPOINT 运行真实 etcd 测试")
	}
	client := newIntegrationClient(t, endpoint)
	prefix := uniqueIntegrationKey(t, "member-guard") + "/"
	t.Cleanup(func() { _, _ = client.raw.Delete(context.Background(), prefix, clientv3.WithPrefix()) })
	ctx := t.Context()
	member, ok, err := client.Create(ctx, prefix+"member", []byte("running"), 0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	snapshot, err := client.Snapshot(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Delete(ctx, member.Key); err != nil {
		t.Fatal(err)
	}
	ok, err = client.CompareAndSwapWithinSnapshot(ctx, prefix, snapshot.Revision, prefix+"slot", 0, []byte("assigned"), member)
	if err != nil || ok {
		t.Fatalf("成员已删除仍写入归属: %t %v", ok, err)
	}
}
