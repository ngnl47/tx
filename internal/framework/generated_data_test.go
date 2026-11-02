package framework

import (
	"testing"
	"time"

	"github.com/ngnl47/tx/meta"
)

func TestFusionMetadataValidationDoesNotRequireReaderBinding(t *testing.T) {
	server := &Server{types: meta.NewRegistry()}
	validate := server.fusionCMod().(CModValidator)
	if err := validate.Validate(nil); err != nil {
		t.Fatalf("类型元数据已提供时不应要求 Reader 绑定: %v", err)
	}
	server.config.Runtime.Fusion.DirtyTTL = -time.Second
	if err := validate.Validate(nil); err == nil {
		t.Fatal("仍须校验 Fusion 的持久化参数")
	}
	server.config.Runtime.Fusion.DirtyTTL = 0
	server.types = nil
	if err := validate.Validate(nil); err == nil {
		t.Fatal("缺少类型元数据应拒绝启用 Fusion")
	}
}
