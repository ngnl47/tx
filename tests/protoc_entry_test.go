package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProtocEntryRejectsWrongCompilerVersion(t *testing.T) {
	compiler := filepath.Join(t.TempDir(), "protoc")
	// 替身仅报告旧版本；生成入口必须在调用 Go 和插件前拒绝它。
	if err := os.WriteFile(compiler, []byte("#!/bin/sh\necho 'libprotoc 29.3'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROTOC", compiler)
	t.Setenv("GO_BIN", filepath.Join(t.TempDir(), "must-not-run-go"))
	cmd := exec.Command("bash", "../cmd/protoc/run.sh", "actor")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("旧版 protoc 未被拒绝")
	}
	for _, version := range []string{"libprotoc 36.0", "libprotoc 29.3"} {
		if !strings.Contains(string(output), version) {
			t.Fatalf("错误应说明要求版本和实际版本，缺少 %q: %s", version, output)
		}
	}
}
