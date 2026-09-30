package actorhost

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// 定时器未入队时遇到暂时背压不能断掉业务 Tick 链；关闸后也不能越过准入继续投递。
func TestAfterWaitsForMailboxCapacityUntilDrain(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run(map[bool]string{false: "capacity_recovers", true: "drain"}[drain], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, release, ticked := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var handle Context
				runtime := New(1, time.Second, func(ctx Context) (*Behavior, error) {
					handle = ctx
					return &Behavior{Receive: func(_ Context, value any) (any, error) {
						if value == "block" {
							close(entered)
							<-release
						}
						if value == "tick" {
							close(ticked)
						}
						return nil, nil
					}}, nil
				})
				defer runtime.Drain(context.Background())
				id := Identity{Kind: "room", Key: "full"}
				if _, err := runtime.Submit(id, "block"); err != nil {
					t.Fatal(err)
				}
				<-entered
				if _, err := runtime.Submit(id, "queued"); err != nil {
					t.Fatal(err)
				}
				handle.After(0, "tick")
				synctest.Wait()
				time.Sleep(20 * time.Millisecond)
				if drain {
					runtime.BeginDrain()
				}
				close(release)
				time.Sleep(20 * time.Millisecond)
				synctest.Wait()
				select {
				case <-ticked:
					if drain {
						t.Fatal("timer bypassed closed admission")
					}
				default:
					if !drain {
						t.Fatal("timer lost after temporary mailbox saturation")
					}
				}
			})
		})
	}
}
