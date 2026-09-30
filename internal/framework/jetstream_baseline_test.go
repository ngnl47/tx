package framework

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ngnl47/tx/internal/taskqueue"
	"github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/tasks"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 独立性能入口；所有计时、采样和任务核对均不进入生产路径。
func TestJetStreamTaskBaseline(t *testing.T) {
	if os.Getenv("TX_TASK_BASELINE") != "1" {
		t.Skip("显式启用独立任务基线")
	}
	url, endpoint := os.Getenv("TX_ACTORTRIAL_NATS_URL"), os.Getenv("TX_ETCD_ENDPOINT")
	if url == "" || endpoint == "" {
		t.Fatal("基线必须提供隔离 NATS 和 etcd 地址")
	}
	const roles = 32
	parallel, batch, native := 4, 1, false
	profile := os.Getenv("TX_TASK_PROFILE")
	switch profile {
	case "", "actor4":
		profile = "actor4"
	case "native4":
		native = true
	case "native32":
		parallel, native = 32, true
	case "native32batch":
		parallel, batch, native = 32, 16, true
	case "actor32batch":
		parallel, batch = 32, 16
	default:
		t.Fatal("未知 TX_TASK_PROFILE")
	}
	count, rate := 1000, 0
	if value := os.Getenv("TX_TASK_RATE"); value != "" {
		var err error
		rate, err = strconv.Atoi(value)
		if err != nil || rate <= 0 || rate > 10000 {
			t.Fatal("TX_TASK_RATE 必须为 1～10000 条/s")
		}
		count = rate * 5
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	owner := rand.Text()
	prefix := "/tx/tests/task-baseline/" + owner
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	marker := prefix + "/owner"
	created, err := client.Txn(ctx).If(clientv3.Compare(clientv3.Version(marker), "=", 0)).Then(clientv3.OpPut(marker, owner)).Commit()
	if err != nil || !created.Succeeded {
		t.Fatalf("基线归属: %v", err)
	}
	defer func() {
		if t.Failed() {
			t.Log("保留失败 etcd 范围", prefix)
			return
		}
		clean, end := context.WithTimeout(context.Background(), time.Second)
		defer end()
		result, err := client.Txn(clean).If(clientv3.Compare(clientv3.Value(marker), "=", owner)).Then(clientv3.OpDelete(prefix+"/", clientv3.WithPrefix())).Commit()
		if err != nil || !result.Succeeded {
			t.Errorf("清理基线归属: %v", err)
		}
	}()
	nc, err := nats.Connect(url, nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int64
	kind := actor.MustKind("role", func(actor.Context) (*actor.Behavior, error) {
		var balance int64
		return &actor.Behavior{Receive: func(actor.Context, any) (any, error) {
			balance++
			executions.Add(1)
			return actor.Result{MessageType: "reward.done", Payload: []byte(strconv.FormatInt(balance, 10))}, nil
		}}, nil
	})
	var access actor.Runtime
	services := []Service{testService{spec: Spec{Name: "game", ActorKinds: []actor.Kind{kind}}}, testService{spec: Spec{Name: "worker", Requires: []CModID{CModActor}}, start: func(c Context) error { access = c.Actor(); return nil }}}
	start := func(name string) *Server {
		app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", services...)
		app.config.Actor.Runtime = actor.Config{NATSURL: url, Namespace: owner}
		if err := app.Start(ctx, RunOptions{Services: []string{name}, RPCListen: "127.0.0.1:0"}); err != nil {
			t.Fatal(err)
		}
		return app
	}
	game := start("game")
	defer game.Stop(context.Background())
	worker := start("worker")
	defer worker.Stop(context.Background())
	for len(worker.discovery.Directory().RunningInstances("game")) != 1 {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// 归属建立与 Actor 加载不计入热链路窗口。
	for i := 0; i < roles; i++ {
		if _, err := access.Call(ctx, actor.Identity{Kind: "role", Key: strconv.Itoa(i)}, actor.Delivery{MessageType: "warm"}); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 3; round++ {
		for _, replicas := range []int{1, 3} {
			name := fmt.Sprintf("tx_baseline_%s_%d_%d", owner, round, replicas)
			stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy, Replicas: replicas, Metadata: map[string]string{"owner": owner}})
			if err != nil {
				t.Fatal(err)
			}
			runErr := func() error {
				consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxAckPending: parallel * batch, Replicas: replicas})
				if err != nil {
					return err
				}
				publisher := &taskRuntime{js: js, config: tasks.Config{Stream: name, Subject: name}}
				processor := taskqueue.Processor{Actor: access, JetStream: js, Timeout: 30 * time.Second}
				starts := make([]time.Time, count)
				pubTimes := make([]time.Duration, count)
				doneTimes := make([]time.Duration, count)
				plannedTimes := make([]time.Duration, count)
				startLags := make([]time.Duration, count)
				var begin time.Time
				var lastCompletion time.Time
				var mu sync.Mutex
				var finished, started int
				completed := make(chan struct{})
				failures := make(chan error, count)
				var consumers []jetstream.ConsumeContext
				defer func() {
					for _, c := range consumers {
						c.Drain()
					}
					for _, c := range consumers {
						<-c.Closed()
					}
				}()
				for i := 0; i < parallel; i++ {
					c, err := consumer.Consume(func(msg jetstream.Msg) {
						// 原生对照保留相同信封解码和逐条确认，只移除 Actor 调用及其续期适配。
						var processErr error
						if native {
							var task taskqueue.Task
							processErr = json.Unmarshal(msg.Data(), &task)
							if processErr == nil {
								executions.Add(1)
								processErr = msg.DoubleAck(ctx)
							}
						} else {
							processErr = processor.Process(ctx, msg, 10*time.Second)
						}
						if err := processErr; err != nil {
							failures <- err
							return
						}
						ended := time.Now()
						var task taskqueue.Task
						if err := json.Unmarshal(msg.Data(), &task); err != nil {
							failures <- err
							return
						}
						if len(task.Delivery.Payload) != 256 {
							failures <- fmt.Errorf("任务长度不符")
							return
						}
						index := int(binary.LittleEndian.Uint64(task.Delivery.Payload))
						meta, err := msg.Metadata()
						if err != nil || meta.NumDelivered != 1 || index < 0 || index >= count {
							failures <- fmt.Errorf("任务重投或标识不符: %v", err)
							return
						}
						mu.Lock()
						defer mu.Unlock()
						if doneTimes[index] != 0 {
							failures <- fmt.Errorf("重复完成 %d", index)
							return
						}
						doneTimes[index] = ended.Sub(starts[index])
						if rate > 0 {
							plannedTimes[index] = ended.Sub(begin.Add(time.Duration(index) * time.Second / time.Duration(rate)))
						}
						if ended.After(lastCompletion) {
							lastCompletion = ended
						}
						finished++
						if finished == count {
							close(completed)
						}
					}, jetstream.PullMaxMessages(batch))
					if err != nil {
						return err
					}
					consumers = append(consumers, c)
				}
				// 单独 warm Actor 后再开始窗口；窗口包含首条拉取及队列等待。
				before := executions.Load()
				begin = time.Now()
				windowObserved := make(chan struct{})
				var unfinishedAtEnd, notStartedAtEnd int
				if rate > 0 {
					go func() {
						defer close(windowObserved)
						timer := time.NewTimer(time.Until(begin.Add(5 * time.Second)))
						defer timer.Stop()
						select {
						case <-timer.C:
						case <-ctx.Done():
							return
						}
						mu.Lock()
						defer mu.Unlock()
						unfinishedAtEnd, notStartedAtEnd = count-finished, count-started
					}()
				} else {
					close(windowObserved)
				}
				var publishing sync.WaitGroup
				for p := 0; p < parallel; p++ {
					publishing.Add(1)
					go func(p int) {
						defer publishing.Done()
						for i := p; i < count; i += parallel {
							var planned time.Time
							if rate > 0 {
								planned = begin.Add(time.Duration(i) * time.Second / time.Duration(rate))
								if delay := time.Until(planned); delay > 0 {
									timer := time.NewTimer(delay)
									select {
									case <-timer.C:
									case <-ctx.Done():
										timer.Stop()
										failures <- ctx.Err()
										return
									}
								}
							}
							data := make([]byte, 256)
							binary.LittleEndian.PutUint64(data, uint64(i))
							mu.Lock()
							starts[i] = time.Now()
							began := starts[i]
							started++
							if rate > 0 {
								startLags[i] = began.Sub(planned)
							}
							mu.Unlock()
							err := publisher.Publish(ctx, actor.Identity{Kind: "role", Key: strconv.Itoa(i % roles)}, actor.Delivery{MessageType: "reward", Payload: data})
							pubTimes[i] = time.Since(began)
							if err != nil {
								failures <- err
								return
							}
						}
					}(p)
				}
				publishing.Wait()
				publishedAt := time.Now()
				select {
				case err := <-failures:
					return err
				case <-ctx.Done():
					return ctx.Err()
				case <-completed:
				}
				mu.Lock()
				elapsed := time.Since(begin)
				mu.Unlock()
				select {
				case err := <-failures:
					return err
				default:
				}
				if executions.Load()-before != int64(count) {
					return fmt.Errorf("业务执行次数不符")
				}
				// R3 ACK 确认后源流清理状态可能稍后收敛；不把查询等待混入完成计时。
				settle, end := context.WithTimeout(ctx, 5*time.Second)
				defer end()
				var info *jetstream.StreamInfo
				for {
					info, err = stream.Info(settle)
					if err != nil {
						return err
					}
					if info.State.Msgs == 0 {
						break
					}
					select {
					case <-time.After(10 * time.Millisecond):
					case <-settle.Done():
						return fmt.Errorf("确认后源流未收敛: %d", info.State.Msgs)
					}
				}
				slices.Sort(pubTimes)
				slices.Sort(doneTimes)
				percentile := func(values []time.Duration, p int) float64 {
					return float64(values[(len(values)*p+99)/100-1]) / float64(time.Millisecond)
				}
				record := map[string]any{"profile": profile, "pull_batch": batch, "max_ack_pending": parallel * batch, "round": round + 1, "replicas": replicas, "tasks": count, "roles": roles, "payload_bytes": 256, "publishers": parallel, "consumers": parallel, "publish_per_second": float64(count) / publishedAt.Sub(begin).Seconds(), "complete_per_second": float64(count) / elapsed.Seconds(), "publish_p50_ms": percentile(pubTimes, 50), "publish_p99_ms": percentile(pubTimes, 99), "complete_p50_ms": percentile(doneTimes, 50), "complete_p99_ms": percentile(doneTimes, 99), "executions": executions.Load() - before, "remaining": info.State.Msgs}
				if rate > 0 {
					<-windowObserved
					slices.Sort(plannedTimes)
					slices.Sort(startLags)
					record["planned_rate"] = rate
					record["window_seconds"] = 5
					record["planned_complete_p99_ms"] = percentile(plannedTimes, 99)
					record["start_lag_p99_ms"] = percentile(startLags, 99)
					record["unfinished_at_window_end"] = unfinishedAtEnd
					record["not_started_at_window_end"] = notStartedAtEnd
					record["drain_ms"] = float64(max(time.Duration(0), lastCompletion.Sub(begin.Add(5*time.Second)), publishedAt.Sub(begin.Add(5*time.Second)))) / float64(time.Millisecond)
				}
				encoded, err := json.Marshal(record)
				if err != nil {
					return err
				}
				t.Log("TASK_BASELINE " + string(encoded))
				return nil
			}()
			if runErr != nil {
				t.Fatalf("保留失败流 %s: %v", name, runErr)
			}
			info, err := stream.Info(ctx)
			if err != nil || info.Config.Metadata["owner"] != owner {
				t.Fatalf("流归属不符: %v", err)
			}
			if err := js.DeleteStream(ctx, name); err != nil {
				t.Fatal(err)
			}
		}
	}
}
