package sessionid

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewRejectsInvalidWorkSlot(t *testing.T) {
	for _, workSlot := range []uint16{0, 4096} {
		if _, err := New(workSlot); !errors.Is(err, ErrInvalidWorkSlot) {
			t.Fatalf("New(%d) error = %v, want ErrInvalidWorkSlot", workSlot, err)
		}
	}
}

func TestNewAllocatesWithConfiguredWorkSlot(t *testing.T) {
	generator, err := New(17)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	id, err := generator.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("AllocateSessionID: %v", err)
	}
	if workSlotBits(id) != 17 {
		t.Fatalf("workSlot bits = %d, want 17", workSlotBits(id))
	}
}

func TestGeneratorBuildsJavaScriptSafeIDs(t *testing.T) {
	generator := newGeneratorForTest(4095, time.Unix(EpochUnix+(1<<29)-1, 0))
	seen := make(map[int64]struct{}, 4096)
	var last int64
	for range 4096 {
		id, err := generator.AllocateSessionID(context.Background())
		if err != nil {
			t.Fatalf("AllocateSessionID: %v", err)
		}
		if id <= 0 || id > MaxSessionID {
			t.Fatalf("id = %d, want JavaScript safe positive integer", id)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id = %d", id)
		}
		seen[id] = struct{}{}
		last = id
	}
	if last != MaxSessionID {
		t.Fatalf("last id = %d, want MaxSessionID %d", last, MaxSessionID)
	}
}

func TestGeneratorWrapsTimestampBits(t *testing.T) {
	clock := newFakeClock(time.Unix(EpochUnix+(1<<29)-1, 0))
	generator := newGeneratorWithClock(17, clock.Now, clock.Wait)
	last, err := generator.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("allocate last timestamp: %v", err)
	}
	clock.Set(time.Unix(EpochUnix+(1<<29), 0))
	first, err := generator.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("allocate wrapped timestamp: %v", err)
	}
	if timestampBits(last) != (1<<29)-1 || timestampBits(first) != 0 {
		t.Fatalf("timestamp bits = %d and %d, want %d and 0", timestampBits(last), timestampBits(first), (1<<29)-1)
	}
}

func TestGeneratorsWithDifferentWorkSlotsDoNotCollide(t *testing.T) {
	now := time.Unix(EpochUnix+47, 0)
	first := newGeneratorForTest(17, now)
	second := newGeneratorForTest(18, now)
	firstID, err := first.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("allocate first workSlot: %v", err)
	}
	secondID, err := second.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("allocate second workSlot: %v", err)
	}
	if firstID == secondID {
		t.Fatalf("different workSlots produced duplicate id %d", firstID)
	}
}

func TestGeneratorRejectsClockBeforeEpoch(t *testing.T) {
	generator := newGeneratorForTest(17, time.Unix(EpochUnix-1, 0))
	if _, err := generator.AllocateSessionID(context.Background()); !errors.Is(err, ErrClockBeforeEpoch) {
		t.Fatalf("AllocateSessionID error = %v, want ErrClockBeforeEpoch", err)
	}
}

func TestGeneratorWaitsForNextSecondAfterSequenceExhaustion(t *testing.T) {
	clock := newFakeClock(time.Unix(EpochUnix+20, 0))
	generator := newGeneratorWithClock(17, clock.Now, clock.Wait)
	for range 4096 {
		if _, err := generator.AllocateSessionID(context.Background()); err != nil {
			t.Fatalf("fill sequence: %v", err)
		}
	}
	id, err := generator.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("allocate after sequence exhaustion: %v", err)
	}
	if timestampBits(id) != 21 || sequenceBits(id) != 0 {
		t.Fatalf("id bits = timestamp %d sequence %d, want 21 and 0", timestampBits(id), sequenceBits(id))
	}
	if waits := clock.Waits(); len(waits) != 1 || waits[0] != time.Second {
		t.Fatalf("waits = %v, want [1s]", waits)
	}
}

func TestGeneratorWaitsForClockRollbackToCatchUp(t *testing.T) {
	clock := newFakeClock(time.Unix(EpochUnix+20, 0))
	generator := newGeneratorWithClock(17, clock.Now, clock.Wait)
	first, err := generator.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("allocate before rollback: %v", err)
	}
	clock.Set(time.Unix(EpochUnix+19, 0))
	second, err := generator.AllocateSessionID(context.Background())
	if err != nil {
		t.Fatalf("allocate after rollback: %v", err)
	}
	if timestampBits(second) != timestampBits(first) || sequenceBits(second) != 1 {
		t.Fatalf("second id bits = timestamp %d sequence %d, want timestamp %d sequence 1", timestampBits(second), sequenceBits(second), timestampBits(first))
	}
	if waits := clock.Waits(); len(waits) != 1 || waits[0] != time.Second {
		t.Fatalf("waits = %v, want [1s]", waits)
	}
}

func TestGeneratorSequenceWaitHonorsContext(t *testing.T) {
	clock := newFakeClock(time.Unix(EpochUnix+20, 0))
	generator := newGeneratorWithClock(17, clock.Now, contextWait)
	for range 4096 {
		if _, err := generator.AllocateSessionID(context.Background()); err != nil {
			t.Fatalf("fill sequence: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := generator.AllocateSessionID(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("AllocateSessionID error = %v, want context canceled", err)
	}
}

func TestGeneratorClockRollbackWaitHonorsContext(t *testing.T) {
	clock := newFakeClock(time.Unix(EpochUnix+20, 0))
	generator := newGeneratorWithClock(17, clock.Now, contextWait)
	if _, err := generator.AllocateSessionID(context.Background()); err != nil {
		t.Fatalf("allocate before rollback: %v", err)
	}
	clock.Set(time.Unix(EpochUnix+19, 0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := generator.AllocateSessionID(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("AllocateSessionID error = %v, want context canceled", err)
	}
}

func TestGeneratorConcurrentAllocationHasNoDuplicates(t *testing.T) {
	generator := newGeneratorForTest(17, time.Unix(EpochUnix+47, 0))
	const count = 1024
	ids := make(chan int64, count)
	errs := make(chan error, count)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(count)
	for range count {
		go func() {
			defer group.Done()
			<-start
			id, err := generator.AllocateSessionID(context.Background())
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	close(start)
	group.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("AllocateSessionID: %v", err)
	}
	seen := make(map[int64]struct{}, count)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id = %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("allocated %d unique IDs, want %d", len(seen), count)
	}
}

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	waits []time.Duration
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func (clock *fakeClock) Wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clock.mu.Lock()
	clock.waits = append(clock.waits, duration)
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
	return nil
}

func (clock *fakeClock) Waits() []time.Duration {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return append([]time.Duration(nil), clock.waits...)
}

func newGeneratorForTest(workSlot uint16, now time.Time) *Generator {
	clock := newFakeClock(now)
	return newGeneratorWithClock(workSlot, clock.Now, clock.Wait)
}

func newGeneratorWithClock(workSlot uint16, now func() time.Time, wait func(context.Context, time.Duration) error) *Generator {
	return &Generator{workSlot: workSlot, now: now, wait: wait}
}

func timestampBits(id int64) int64 {
	return id >> 24
}

func sequenceBits(id int64) int64 {
	return id & ((1 << 12) - 1)
}

func workSlotBits(id int64) int64 {
	return (id >> 12) & ((1 << 12) - 1)
}

func contextWait(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}
