# tx

`tx` 是运行时基础库，提供注解元数据、变更追踪、Redis/Mongo/Fusion 存储适配和延迟任务运行时能力。

## 文档

- [使用手册](docs/usage.md)
- [实施计划](docs/IMPLEMENTATION_PLAN.md)
- [旧生成流程参考](docs/archive/legacy_generation_workflow.md)
- [变更与存储重写历史计划](docs/archive/tx_change_storage_rewrite_plan.md)

## 验证

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./... -count=1
```
