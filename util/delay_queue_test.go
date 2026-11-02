package util_test

import (
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ngnl47/tx/util"
)

func TestDelayQueueOrdersDueTasksAndRemovesPendingTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := util.NewDelayQueue(10 * time.Millisecond)
		var got []int
		queue.AfterFunc(30*time.Millisecond, func() { got = append(got, 3) })
		queue.AfterFunc(10*time.Millisecond, func() { got = append(got, 1) })
		removed := queue.AfterFunc(10*time.Millisecond, func() { got = append(got, -1) })
		queue.AfterFunc(10*time.Millisecond, func() { got = append(got, 2) })
		if err := queue.Remove(removed); err != nil {
			t.Fatal(err)
		}
		queue.Start()
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		queue.Stop()
		if !reflect.DeepEqual(got, []int{1, 2, 3}) {
			t.Fatalf("execution order = %v", got)
		}
	})
}

func TestDelayQueuePeriodicIntervalAndStableCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := util.NewDelayQueue(10 * time.Millisecond)
		starts := make(chan time.Time, 8)
		id, err := queue.AddScheduled(100*time.Millisecond, func() {
			starts <- time.Now()
			time.Sleep(20 * time.Millisecond)
		})
		if err != nil {
			t.Fatal(err)
		}
		queue.Start()
		defer queue.Stop()
		time.Sleep(60 * time.Millisecond)
		synctest.Wait()
		if len(starts) != 1 {
			t.Fatalf("periodic task ran %d times before interval elapsed, want 1", len(starts))
		}
		first := <-starts
		time.Sleep(170 * time.Millisecond)
		synctest.Wait()
		if len(starts) != 1 {
			t.Fatalf("periodic task ran %d more times, want 1", len(starts))
		}
		if elapsed := (<-starts).Sub(first); elapsed < 120*time.Millisecond {
			t.Fatalf("periodic start interval = %v, want callback duration plus interval", elapsed)
		}
		if err := queue.Remove(id); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if len(starts) != 0 {
			t.Fatalf("periodic task continued after original ID removed: %d calls", len(starts))
		}
	})
}

func TestDelayQueuePeriodicTaskCanCancelItself(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := util.NewDelayQueue(10 * time.Millisecond)
		calls := 0
		var id *util.TaskID
		id, _ = queue.AddScheduled(100*time.Millisecond, func() {
			calls++
			if calls == 2 {
				_ = queue.Remove(id)
			}
		})
		queue.Start()
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		queue.Stop()
		if calls != 2 {
			t.Fatalf("self-cancelled periodic task ran %d times, want 2", calls)
		}
	})
}

func TestDelayQueueCallbackCanScheduleWithBacklog(t *testing.T) {
	queue := util.NewDelayQueue(time.Millisecond)
	const tasks = 11000
	var completed sync.WaitGroup
	completed.Add(tasks + 1)
	queue.AfterFunc(0, func() {
		queue.AfterFunc(0, completed.Done)
		completed.Done()
	})
	for i := 1; i < tasks; i++ {
		queue.AfterFunc(0, completed.Done)
	}
	queue.Start()
	completed.Wait()
	queue.Stop()
}

func TestDelayQueueStopWaitsForDispatchedTasksAndPreventsNewWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := util.NewDelayQueue(10 * time.Millisecond)
		entered, release := make(chan struct{}), make(chan struct{})
		var got []int
		queue.AfterFunc(0, func() {
			close(entered)
			<-release
			got = append(got, 1)
		})
		queue.AfterFunc(0, func() { got = append(got, 2) })
		cancelled := queue.AfterFunc(0, func() { got = append(got, -2) })
		queue.AfterFunc(time.Hour, func() { got = append(got, -1) })
		queue.Start()
		<-entered
		if err := queue.Remove(cancelled); err != nil {
			t.Fatal(err)
		}
		firstStop, secondStop := make(chan struct{}), make(chan struct{})
		go func() { queue.Stop(); close(firstStop) }()
		synctest.Wait()
		go func() { queue.Stop(); close(secondStop) }()
		synctest.Wait()
		for _, stopped := range []chan struct{}{firstStop, secondStop} {
			select {
			case <-stopped:
				t.Error("Stop returned before callback completed")
			default:
			}
		}
		if _, err := queue.Add(0, func() {}); err == nil {
			t.Error("Add accepted work while stopping")
		}
		close(release)
		<-firstStop
		<-secondStop
		if !reflect.DeepEqual(got, []int{1, 2}) {
			t.Fatalf("Stop completed tasks = %v, want dispatched tasks only", got)
		}
		queue.Start()
		if _, err := queue.AddScheduled(time.Second, func() {}); err == nil {
			t.Error("terminal queue accepted periodic work")
		}
		queue.Stop()
	})
}
