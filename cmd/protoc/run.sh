#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
go_bin="${GO_BIN:-go}"

usage() {
    cat <<'EOF'
用法: cmd/protoc/run.sh [--all|--list|包名...]

可生成包:
  actor  modules/actor/proto/actor.proto
  msgq   modules/msgq/proto/msgq.proto
  rpc    modules/rpc/proto/rpc.proto

示例:
  ./cmd/protoc/run.sh actor
  ./cmd/protoc/run.sh --all

发布流程可只传入需要导出的包名，从而跳过其他 proto 包。
环境变量:
  PROTOC  指定 protoc 可执行文件。
  GO_BIN  指定用于定位 protoc 插件和读取模块名的 go 可执行文件。
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

if [[ "${1:-}" == "--list" ]]; then
    printf '%s\n' actor msgq rpc
    exit 0
fi

if [[ $# -eq 0 || "${1:-}" == "--all" ]]; then
    set -- actor msgq rpc
fi

if [[ -n "${PROTOC:-}" ]]; then
    protoc_bin="$PROTOC"
else
    protoc_bin="$(command -v protoc || true)"
fi
if [[ -z "$protoc_bin" || ! -x "$protoc_bin" ]]; then
    printf '找不到 protoc；请设置 PROTOC 或将它加入 PATH。\n' >&2
    exit 1
fi

go_bin_dir="$($go_bin env GOBIN)"
if [[ -z "$go_bin_dir" ]]; then
    go_bin_dir="$($go_bin env GOPATH)/bin"
fi
export PATH="$go_bin_dir:$PATH"

for plugin in protoc-gen-go protoc-gen-go-grpc; do
    if ! command -v "$plugin" >/dev/null 2>&1; then
        printf '找不到 %s；请使用 %s install 安装。\n' "$plugin" "$go_bin" >&2
        exit 1
    fi
done

cd "$root_dir"
module_path="$($go_bin list -m -f '{{.Path}}')"

proto_files=()
for package_name in "$@"; do
    case "$package_name" in
        actor)
            proto_files+=("modules/actor/proto/actor.proto")
            ;;
        msgq)
            proto_files+=("modules/msgq/proto/msgq.proto")
            ;;
        rpc)
            proto_files+=("modules/rpc/proto/rpc.proto")
            ;;
        *)
            printf '未知 proto 包: %s\n' "$package_name" >&2
            usage >&2
            exit 1
            ;;
    esac
done

"$protoc_bin" \
    -I "$root_dir" \
    --go_out="$root_dir" \
    --go_opt="module=$module_path" \
    --go-grpc_out="$root_dir" \
    --go-grpc_opt="module=$module_path" \
    "${proto_files[@]}"
