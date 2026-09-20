package taskqueue

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// 仅在独立 Docker 网络内验证 R3 单节点故障，不代表跨宿主容灾。
func TestRecoveryAfterClusterLeaderLoss(t *testing.T) {
	image := os.Getenv("TX_TASKQUEUE_RESTART_IMAGE")
	if image == "" {
		t.Skip("设置本地 NATS 镜像验证隔离 R3 集群")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	dir, err := os.MkdirTemp("", "tx-task-r3-")
	if err != nil {
		t.Fatal(err)
	}
	network := "tx-task-r3-" + strings.ToLower(rand.Text())
	var containers []string
	networkCreated := false
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("保留失败集群 network=%s containers=%v config=%s", network, containers, dir)
			return
		}
		cleanup, end := context.WithTimeout(context.Background(), 20*time.Second)
		defer end()
		if len(containers) > 0 {
			if out, err := exec.CommandContext(cleanup, "docker", append([]string{"rm", "-f"}, containers...)...).CombinedOutput(); err != nil {
				t.Errorf("清理测试容器: %v %s", err, out)
				return
			}
		}
		if networkCreated {
			if out, err := exec.CommandContext(cleanup, "docker", "network", "rm", network).CombinedOutput(); err != nil {
				t.Errorf("清理测试网络: %v %s", err, out)
				return
			}
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	docker("network", "create", network)
	networkCreated = true
	var urls []string
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("node%d", i)
		config := fmt.Sprintf("server_name: %s\nport: 4222\njetstream {store_dir: /data, max_file_store: 64MB, sync_interval: always}\ncluster {name: task-r3, port: 6222, routes: [nats://node0:6222,nats://node1:6222,nats://node2:6222]}\n", name)
		path := filepath.Join(dir, name+".conf")
		if err := os.WriteFile(path, []byte(config), 0644); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		id := docker("create", "--pull=never", "--name", network+"-"+name, "--network", network, "--network-alias", name, "-p", address+":4222", "--mount", "type=bind,src="+path+",dst=/etc/nats/test.conf,readonly", image, "-c", "/etc/nats/test.conf")
		containers = append(containers, id)
		urls = append(urls, "nats://"+address)
	}
	docker(append([]string{"start"}, containers...)...)
	wait := func(check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	var nc *nats.Conn
	wait(func() bool {
		nc, err = nats.Connect(strings.Join(urls, ","), nats.IgnoreDiscoveredServers(), nats.ReconnectWait(25*time.Millisecond), nats.Timeout(200*time.Millisecond))
		return err == nil
	})
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	// 等待元数据组就绪；重复创建仅用于本测试的同名同配置资源。
	var source jetstream.Stream
	wait(func() bool {
		attempt, end := context.WithTimeout(ctx, time.Second)
		defer end()
		source, err = js.CreateStream(attempt, jetstream.StreamConfig{Name: "TASKS", Subjects: []string{"tasks"}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy, Replicas: 3})
		return err == nil
	})
	create := func(name, subject string, retention jetstream.RetentionPolicy) jetstream.Stream {
		t.Helper()
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subject}, Storage: jetstream.FileStorage, Retention: retention, Replicas: 3})
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
	events := create("EVENTS", MaxDeliverySubject("TASKS", "worker"), jetstream.WorkQueuePolicy)
	dead := create("DEAD", "dead", jetstream.LimitsPolicy)
	consumer, err := source.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 200 * time.Millisecond, MaxDeliver: 1, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	notifications, err := events.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "recovery", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"Identity":{"Kind":"room","Key":"expired"},"Delivery":{"MessageType":"buff"}}`)
	first, err := js.Publish(ctx, "tasks", data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, "dead", []byte("existing-dead")); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(jetstream.FetchMaxWait(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(jetstream.FetchMaxWait(500 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("未耗尽: %v", err)
	}
	event, err := notifications.Next(jetstream.FetchMaxWait(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	current := func(cluster *jetstream.ClusterInfo) bool {
		if cluster == nil || cluster.Leader == "" || len(cluster.Replicas) != 2 {
			return false
		}
		for _, replica := range cluster.Replicas {
			if !replica.Current || replica.Offline {
				return false
			}
		}
		return true
	}
	healthy := func() bool {
		attempt, end := context.WithTimeout(ctx, time.Second)
		defer end()
		for _, stream := range []jetstream.Stream{source, events, dead} {
			info, err := stream.Info(attempt)
			if err != nil || !current(info.Cluster) {
				return false
			}
		}
		for _, c := range []jetstream.Consumer{consumer, notifications} {
			info, err := c.Info(attempt)
			if err != nil || !current(info.Cluster) {
				return false
			}
		}
		return true
	}
	wait(healthy)
	info, err := source.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	leader := info.Cluster.Leader
	index := -1
	for i := 0; i < 3; i++ {
		if leader == fmt.Sprintf("node%d", i) {
			index = i
		}
	}
	if index < 0 {
		t.Fatalf("未知测试 leader: %s", leader)
	}
	docker("kill", "--signal=KILL", containers[index])
	wait(func() bool {
		attempt, end := context.WithTimeout(ctx, time.Second)
		defer end()
		info, err := source.Info(attempt)
		return err == nil && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != leader
	})
	// 新 leader 出现不代表所有 consumer 已完成选举；只读等待其 leader 切换。
	wait(func() bool {
		attempt, end := context.WithTimeout(ctx, time.Second)
		defer end()
		for _, c := range []jetstream.Consumer{consumer, notifications} {
			info, err := c.Info(attempt)
			if err != nil || info.Cluster == nil || info.Cluster.Leader == "" || info.Cluster.Leader == leader {
				return false
			}
		}
		for _, stream := range []jetstream.Stream{events, dead} {
			info, err := stream.Info(attempt)
			if err != nil || info.Cluster == nil || info.Cluster.Leader == "" || info.Cluster.Leader == leader {
				return false
			}
		}
		return true
	})
	second, err := js.Publish(ctx, "tasks", []byte("accepted-with-two-nodes"))
	if err != nil {
		t.Fatalf("剩余两节点不能确认新发布: %v", err)
	}
	recovery := Recovery{JetStream: js, Source: source, Consumer: consumer, DeadLetter: "dead"}
	if err := recovery.Process(ctx, event); err != nil {
		t.Fatalf("剩余两节点无法完成恢复: %v", err)
	}
	// 再失去一个节点时已无多数派，调用方不得收到成功发布确认。
	other := (index + 1) % len(containers)
	docker("kill", "--signal=KILL", containers[other])
	attempt, end := context.WithTimeout(ctx, time.Second)
	_, publishErr := js.Publish(attempt, "tasks", []byte("quorum-result-unknown"))
	end()
	if publishErr == nil {
		t.Fatal("失去多数派仍返回成功发布确认")
	}
	// 错误不能证明消息没写入；不自动重发，也不据此断言流里没有该消息。
	t.Logf("失去多数派的发布未获确认: %v", publishErr)
	docker("start", containers[index], containers[other])
	wait(healthy)
	if _, err := source.GetMsg(ctx, first.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("耗尽源残留未删除: %v", err)
	}
	stored, err := source.GetMsg(ctx, second.Sequence)
	if err != nil || string(stored.Data) != "accepted-with-two-nodes" {
		t.Fatalf("缺失故障期间已确认任务: %+v %v", stored, err)
	}
	deadInfo, err := dead.Info(ctx)
	if err != nil || deadInfo.State.Msgs != 2 {
		t.Fatalf("死信计数不符: %+v %v", deadInfo, err)
	}
	stored, err = dead.GetMsg(ctx, deadInfo.State.LastSeq)
	if err != nil || string(stored.Data) != string(data) || stored.Header.Get("Task-Sequence") != fmt.Sprint(first.Sequence) {
		t.Fatalf("死信内容不符: %+v %v", stored, err)
	}
	eventInfo, err := events.Info(ctx)
	if err != nil || eventInfo.State.Msgs != 0 {
		t.Fatalf("通知未确认: %+v %v", eventInfo, err)
	}
	t.Logf("R3 leader=%s SIGKILL；剩余两节点发布与转存确认成功；再失去多数派不返回成功确认；节点重返后全部流和 durable 副本追平", leader)
}
