//go:build !txbench

package runtime

import (
	"context"
	"testing"
)

func TestDefaultBuildDisablesIOCollection(t *testing.T) {
	runtime := New(context.Background())
	if runtime.io.mongoMonitor() != nil {
		t.Fatal("普通构建不应安装 Mongo 采集回调")
	}
	if stats := runtime.IOStats(); len(stats) != 0 {
		t.Fatalf("普通构建不应产生压测统计: %v", stats)
	}
}
