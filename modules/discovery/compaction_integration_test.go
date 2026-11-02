package discovery

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	txetcd "github.com/ngnl47/tx/etcd"
	"github.com/ngnl47/tx/sessionid"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdRuntimeRecoversAfterCompaction(t *testing.T) {
	// 压缩影响整个集群；此开关只能指向本轮独占的 etcd，不能回退共享端点。
	endpoint := os.Getenv("TX_ETCD_COMPACTION_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ETCD_COMPACTION_ENDPOINT 指向独占 etcd 后运行压缩验收")
	}
	for _, target := range []string{"instances", "slots"} {
		t.Run(target, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			raw, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := raw.Close(); err != nil {
					t.Errorf("关闭 etcd 客户端: %v", err)
				}
			})
			root := "/tx/tests/compaction/" + uuid.NewString()
			memberPrefix, slotPrefix := root+"/instances/", root+"/slots/"
			issuer := root + "-session-ids/issuer"
			gate := &compactionClientGate{
				KV: raw.KV, Watcher: raw.Watcher, target: root + "/" + target + "/",
				firstWatch: make(chan int64, 1), resumeWatch: make(chan struct{}),
				compacted: make(chan clientv3.WatchResponse, 1), watches: make(chan string, 8),
				snapshots: make(chan string, 2), reads: make(map[string]int),
				rebuild: map[string]chan struct{}{memberPrefix: make(chan struct{}), slotPrefix: make(chan struct{})},
			}
			raw.KV, raw.Watcher = gate, gate
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				if _, err := gate.KV.Delete(cleanupCtx, root+"/", clientv3.WithPrefix()); err != nil {
					t.Errorf("清理测试目录: %v", err)
				}
				if _, err := gate.KV.Delete(cleanupCtx, issuer); err != nil {
					t.Errorf("清理发号键: %v", err)
				}
			})
			client, err := txetcd.NewWithClient(raw, false)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewRuntimeWithClient(ctx, client, EtcdConfig{
				Endpoints: []string{endpoint}, Prefix: root, LeaseTTL: 5 * time.Second,
			}, "compaction", Instance{Services: []string{"login"}, Endpoint: "127.0.0.1:9190", Status: StatusRunning})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				if err := runtime.Close(cleanupCtx); err != nil {
					t.Errorf("关闭运行身份: %v", err)
				}
			})
			slots := runtime.AuthoritativeSlots().(*EtcdSlotStore)
			initial := SlotAssignment{SlotID: "world-1", RuntimeInstanceID: runtime.Identity().InstanceID, Generation: 1}
			if err := slots.CreateSlot(ctx, initial); err != nil {
				t.Fatal(err)
			}
			allocator, err := sessionid.NewEtcdAllocator(client, issuer)
			if err != nil {
				t.Fatal(err)
			}
			var lastID int64
			allocate := func() int64 {
				t.Helper()
				id, err := allocator.AllocateSessionID(ctx)
				if err != nil || id <= lastID || id > sessionid.MaxSessionID {
					t.Fatalf("SessionID 未递增或越界: previous=%d id=%d err=%v", lastID, id, err)
				}
				lastID = id
				return id
			}
			allocate()
			watchCtx, stopWatch := context.WithCancel(ctx)
			watchDone := make(chan error, 1)
			go func() { watchDone <- runtime.Watch(watchCtx) }()
			t.Cleanup(func() {
				stopWatch()
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				select {
				case err := <-watchDone:
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("Watch 退出: %v", err)
					}
				case <-cleanupCtx.Done():
					t.Error("Watch 未在清理期限内退出")
				}
			})
			for range 2 {
				waitCompactionEvent(t, ctx, gate.watches, "首轮 Watch")
			}
			startRevision := waitCompactionEvent(t, ctx, gate.firstWatch, "延迟的 Watch")
			if err := runtime.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			if got, err := runtime.Slots().ResolveSlot(ctx, initial.SlotID); err != nil || got != initial {
				t.Fatalf("首轮槽位快照: %+v, %v", got, err)
			}
			if err := runtime.MarkDraining(ctx); err != nil {
				t.Fatal(err)
			}
			if err := slots.ReleaseSlot(ctx, initial); err != nil {
				t.Fatal(err)
			}
			compactRevision := allocate()
			if startRevision <= 0 || startRevision > compactRevision {
				t.Fatalf("Watch 版本不在压缩范围: start=%d compact=%d", startRevision, compactRevision)
			}
			if _, err := raw.Compact(ctx, compactRevision, clientv3.WithCompactPhysical()); err != nil {
				t.Fatal(err)
			}
			close(gate.resumeWatch)
			response := waitCompactionEvent(t, ctx, gate.compacted, "真实压缩响应")
			if !errors.Is(response.Err(), rpctypes.ErrCompacted) || response.CompactRevision < startRevision {
				t.Fatalf("未收到目标 Watch 的压缩错误: %+v, %v", response, response.Err())
			}
			for range 2 {
				waitCompactionEvent(t, ctx, gate.snapshots, "恢复快照屏障")
			}
			if _, found := runtime.Directory().LookupInstance(initial.RuntimeInstanceID); found {
				t.Fatal("压缩后仍可读取旧成员目录")
			}
			if _, err := runtime.Slots().ResolveSlot(ctx, initial.SlotID); !errors.Is(err, ErrSlotDirectoryUnavailable) {
				t.Fatalf("压缩后槽位目录未失效: %v", err)
			}
			allocate()
			close(gate.rebuild[memberPrefix])
			close(gate.rebuild[slotPrefix])
			if err := runtime.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			released := SlotAssignment{SlotID: initial.SlotID, Generation: 2}
			if member, found := runtime.Directory().LookupInstance(initial.RuntimeInstanceID); !found || member.Status != StatusDraining {
				t.Fatalf("恢复后的成员不是新快照: %+v, found=%t", member, found)
			}
			if got, err := runtime.Slots().ResolveSlot(ctx, initial.SlotID); err != nil || got != released {
				t.Fatalf("恢复后的槽位不是新快照: %+v, %v", got, err)
			}
			// 再产生两类增量，确认恢复后仍持续消费真实 Watch。
			if err := runtime.MarkRunning(ctx); err != nil {
				t.Fatal(err)
			}
			next := SlotAssignment{SlotID: initial.SlotID, RuntimeInstanceID: initial.RuntimeInstanceID, Generation: 3}
			if err := slots.PromoteSlot(ctx, released, next); err != nil {
				t.Fatal(err)
			}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				member, found := runtime.Directory().LookupInstance(initial.RuntimeInstanceID)
				got, err := runtime.Slots().ResolveSlot(ctx, initial.SlotID)
				if found && member.Status == StatusRunning && err == nil && got == next {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("恢复后未观察到新增量: member=%+v found=%t slot=%+v err=%v", member, found, got, err)
				case <-ticker.C:
				}
			}
			allocate()
			stored, found, err := client.Get(ctx, issuer)
			if err != nil || !found || stored.LeaseID != 0 || stored.ModRevision != lastID {
				t.Fatalf("压缩后的发号键: %+v found=%t err=%v", stored, found, err)
			}
			t.Logf("Watch=%s 起始版本=%d 压缩版本=%d 最终 SessionID=%d", target, startRevision, response.CompactRevision, lastID)
		})
	}
}

// 仅延迟真实调用，不合成响应；让快照后的首次 Watch 确定落在已压缩版本上。
type compactionClientGate struct {
	clientv3.KV
	clientv3.Watcher
	target      string
	first       sync.Once
	firstWatch  chan int64
	resumeWatch chan struct{}
	compacted   chan clientv3.WatchResponse
	watches     chan string
	snapshots   chan string
	mu          sync.Mutex
	reads       map[string]int
	rebuild     map[string]chan struct{}
}

func (g *compactionClientGate) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if release, ok := g.rebuild[key]; ok {
		g.mu.Lock()
		g.reads[key]++
		rebuilding := g.reads[key] == 2
		g.mu.Unlock()
		if rebuilding {
			g.snapshots <- key
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return g.KV.Get(ctx, key, opts...)
}

func (g *compactionClientGate) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	first := false
	if key == g.target {
		g.first.Do(func() { first = true })
	}
	if !first {
		watch := g.Watcher.Watch(ctx, key, opts...)
		select {
		case g.watches <- key:
		case <-ctx.Done():
		}
		return watch
	}
	select {
	case g.watches <- key:
	case <-ctx.Done():
		return nil
	}
	g.firstWatch <- clientv3.OpGet(key, opts...).Rev()
	select {
	case <-g.resumeWatch:
	case <-ctx.Done():
		return nil
	}
	watch := g.Watcher.Watch(ctx, key, opts...)
	forward := make(chan clientv3.WatchResponse)
	go func() {
		defer close(forward)
		for response := range watch {
			if response.Err() != nil {
				g.compacted <- response
			}
			select {
			case forward <- response:
			case <-ctx.Done():
				return
			}
		}
	}()
	return forward
}

func waitCompactionEvent[T any](t *testing.T, ctx context.Context, events <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-events:
		return value
	case <-ctx.Done():
		t.Fatalf("等待%s超时: %v", description, ctx.Err())
		var zero T
		return zero
	}
}
