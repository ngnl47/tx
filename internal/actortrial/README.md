# Actor 归属与 NATS 集成测试

本目录仅提供 NATS、实体路由、槽位交接、真实写回和租约失效的测试及夹具，不提供生产运行时。正式代码位于 internal/actorhost、internal/actorrpc 和 modules/discovery。

ProtoActor 旧运行时、旧 Agent 适配、后端比较程序及其构建脚本已删除，当前工程不支持 ProtoActor。历史实现仅保存在 Git 提交 cc7c569445b583e0f46343365f37ffcb6c5fb300；性能结论与复现边界见 docs/design.md 的历史性能部分。

现有 *_baseline_test.go 是 NATS 测试夹具，仍服务于归属、排水和持久化断言。生产需求不应在此追加实现；测试执行入口为 go test ./internal/actortrial，真实依赖按用例要求显式提供。四项公共 Delivery 协议契约位于 tests/actor_delivery_test.go，直接验证 modules/actor。
