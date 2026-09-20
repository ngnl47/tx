package taskqueue

import (
	"context"
	"crypto/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// 仅故障注入本测试创建的容器；失败保留容器及配置供排查。
func TestMaxDeliverRecoveryAfterBrokerRestart(t *testing.T) {
	image := os.Getenv("TX_TASKQUEUE_RESTART_IMAGE")
	if image == "" {
		t.Skip("设置本地 NATS 镜像验证隔离 Broker 重启")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	dir, err := os.MkdirTemp("", "tx-task-restart-")
	if err != nil {
		t.Fatal(err)
	}
	var container string
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("保留失败容器=%s 配置=%s", container, dir)
			return
		}
		if container != "" {
			cleanup, end := context.WithTimeout(context.Background(), 10*time.Second)
			defer end()
			if out, err := exec.CommandContext(cleanup, "docker", "rm", "-f", container).CombinedOutput(); err != nil {
				t.Errorf("清理本测试容器: %v %s", err, out)
				return
			}
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	config := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(config, []byte("port: 4222\njetstream {\n store_dir: /data\n max_file_store: 64MB\n sync_interval: always\n}\n"), 0644); err != nil {
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
	// 禁止隐式拉取镜像；随机容器名和 loopback 临时端口不触碰共享实例。
	container = docker("create", "--pull=never", "--name", "tx-task-restart-"+strings.ToLower(rand.Text()), "-p", address+":4222", "--mount", "type=bind,src="+config+",dst=/etc/nats/test.conf,readonly", image, "-c", "/etc/nats/test.conf")
	docker("start", container)
	url := "nats://" + address
	for {
		connection, err := nats.Connect(url, nats.NoReconnect(), nats.Timeout(100*time.Millisecond))
		if err == nil {
			connection.Close()
			break
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	runMaxDeliverRecovery(t, url, func() {
		docker("kill", "--signal=KILL", container)
		docker("start", container)
	})
	t.Log("sync_interval=always、FileStorage、R1：SIGKILL 重启后源任务、死信和通知保留，恢复链路继续完成；不代表宿主掉电验证")
}
