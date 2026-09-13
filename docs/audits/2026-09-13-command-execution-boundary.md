# I3i：示例命令执行与 Actor 回包解耦

起点 tx `564e6e1`、txgame `3afc074`、txgameslg `2fd67a6`，六工程工作区干净。本轮串行实施，不提交 Git。

## 原因与边界

Kind 的后端装配已分离，但两个示例仍在 ProtoActor Context 中混合 BON 校验、业务执行、下行发布和回包。直接切换宿主会同时改动这些关键完成语义，因此先将实际业务执行收敛为 `execute(context.Context, txactor.Delivery) (txactor.Result, error)`。

这是项目内部执行方法，不新增公共 SPI 或第二套命令分派器。两个独立示例分别维护，保留当前 NewCommandKind、固定世界工厂和原生高级接口。公开 Behavior/Context 的承接与正式实体宿主切换仍待后续完成。

## 处理顺序

- Receive 解码信封，传入服务生命周期上下文，最后统一回包；BON、命令号、路由键和可信身份校验仍在执行路径中。
- 内部 Call 返回业务结果；Tell 执行失败仍记录错误。
- Agent 命令成功后按角色发布下行，业务或分派错误按发起 Session 发布 System_Error；发布成功后才返回 ProcessedUpSeq。
- 发布失败返回 Internal 错误和零完成序号。下行发布使用同一生命周期上下文，不改用请求等待超时的上下文。

例如角色改名产生响应后，如果 MsgQ 发布失败，上行 Call 不能确认该 UpSeq 已完成。后续接入私有宿主时可以调用同一执行方法，再由传输适配器编码结果，不需要复制这套顺序。

## 验证

- txgame/server 全量：237 通过、3 跳过、0 失败。
- txgameslg/server 全量：332 通过、5 跳过、0 失败。
- 命令包 race：txgame 16 通过、2 跳过，txgameslg 10 通过，无失败。
- 保留原有真实 Actor 回包测试；新增不启动 ActorSystem 的执行测试，验证发布阻塞时不能提前完成、上下文传递及取消导致发布失败时不确认 Seq。
- 首次 txgame 全量运行受沙箱 Go 缓存访问和端口监听限制阻断，获准重跑后通过。原始输出保存在 `/private/tmp/tx-command-execution-20260913/`。
- 未修改 tx 运行代码、生成产物或客户端；未重跑 tx 全量及客户端测试，未运行独立 cluster-e2e。

两示例非测试 Go 合计新增 80 行、删除 116 行，净减 36 行；测试新增 102 行。本轮为职责分离，没有进行 benchmark，不据此宣称性能提升或确认完整链路无偏移。正式后端切换仍须按既有 I2 基线回测。

下一步承接公开 Behavior/Context 与 Kind 工厂，再将唯一实体宿主接入正式 framework；本节点不表示已删除 ProtoActor 或完成 I3。

后置 ponytail-review：Lean already. Ship. 未新增接口、后端开关或跨示例依赖；保留完成边界测试。差异格式检查通过。
