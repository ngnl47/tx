// task-resources 为本地演示预创建 JetStream 资源，不由业务服务启动调用。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type manifest struct {
	Streams   []jetstream.StreamConfig `json:"streams"`
	Consumers []struct {
		Stream string                   `json:"stream"`
		Config jetstream.ConsumerConfig `json:"config"`
	} `json:"consumers"`
}

func run() error {
	url := flag.String("url", "nats://127.0.0.1:14222", "隔离 NATS 地址")
	path := flag.String("config", "deploy/task-demo/tasks.json", "资源清单")
	flag.Parse()
	content, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var resources manifest
	if err := json.Unmarshal(content, &resources); err != nil {
		return err
	}
	conn, err := nats.Connect(*url, nats.Timeout(3*time.Second))
	if err != nil {
		return err
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 先捕获通知，再允许业务消费者投递；Create 不更新不匹配的已有配置。
	for _, config := range resources.Streams {
		if _, err := js.CreateStream(ctx, config); err != nil {
			return fmt.Errorf("创建流 %s: %w", config.Name, err)
		}
	}
	for _, consumer := range resources.Consumers {
		if _, err := js.CreateConsumer(ctx, consumer.Stream, consumer.Config); err != nil {
			return fmt.Errorf("创建消费者 %s/%s: %w", consumer.Stream, consumer.Config.Durable, err)
		}
	}
	fmt.Printf("已确认 %d 条流、%d 个消费者\n", len(resources.Streams), len(resources.Consumers))
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
