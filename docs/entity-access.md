# 统一 GD 实体访问

本页维护统一实体访问契约。模块关系见 [设计总览](design.md#共享实体与独占实体)，实测结果与未验证范围见 [验收报告](entity-access-acceptance.md)。

## 统一入口与选择

- 生成接口统一显式 `ctx`，使用 `data.FindGD*(ctx, ...)` 和 `data.SnapshotGD*(ctx, ...)`，不增加第二套 db.FindGD*、Access 或可选 ctx 包装。
- 工程在 `gen/config.yml` 选择 `data.entityCache`：txgame 为 false，txgameslg 为 true；缺省 false。少数持久根以 `@noEntityCache` 排除，不增加运行期开关或逐 GD 启动登记。
- tx 按私有实体描述与完整 RecordKey 管理唯一对象。键包含类型、存储归属和完整记录身份，不等同于 Actor ID；不同应用/后端不能错误共享对象。
- 受管持久根必须声明现有事务锁能力，生成时检查。内嵌对象跟随根对象；排除缓存不改变其存储模式，也不强制独占数据使用 Procedure。
- 同一存储身份不能因为多个子服务组合启动而出现两份受管对象。EntityCache 不负责跨进程 Actor 归属或分布式唯一性。

## 三条访问路径

| 路径 | 读取与修改 | 持久化 |
| --- | --- | --- |
| 共享缓存实体 | 普通读取 Snapshot；有效 Procedure 内 Find 唯一对象并用 Store 修改 | 提交登记 WriteBack，失败保留待写状态；回滚撤销修改 |
| 未启用缓存或存储权威类型 | 沿用生成查询、条件更新与显式 ctx | 沿用直接存取及既有事务限制 |
| World 独占根 | Mongo 游标加载后由运行时接管唯一 GD，网格邮箱串行修改/保存 | 周期保存与排水最终保存；不接 EntityCache 或 Procedure WriteBack |

SLG 的 GDUser/GDServer 保留存储权威，避免缓存覆盖建角 CAS 与目录条件更新；GDWorld/GDWorldBucket 是独占根。Role、Legion 等共享根使用 Procedure/Snapshot。

```text
普通只读
  → SnapshotGD(ctx, id)
  → 使用权 + 记录锁 → 深复制 → 释放
  → 独立结果，可在锁外读取/序列化

共享修改
  → Procedure → FindGD(tx.Context(), id)
  → 使用权 + 动态纳管/记录锁 → Store 修改
  → 提交登记 WriteBack / 回滚
  → 释放事务锁与使用权

World
  → Mongo 游标 → 独占 GD → 网格邮箱修改
  → 邮箱内等待保存 → 成功后释放脏状态
```

Snapshot 观察同一缓存中的最新状态，包括已提交未落盘的变更；事务内 Snapshot 复用当前事务锁。多实体 Snapshot 不承诺跨实体同时刻一致性。快照不能作为共享实体写入代理，禁止修改后 Save 覆盖缓存。

共享实体的 Create/Remove 在 Procedure 内协调创建占位、读己写、提交与回滚；首次写入采用既有条件创建，失败保留待写状态。删除与在途写回、Fusion 延迟任务协调，不能让旧任务复活已删除记录或覆盖同 ID 的新记录。不承诺跨数据库原子事务或进程宕机 WAL。

## 职责与生成位置

```text
tx/
├── modules/
│   ├── entitycache/        # 唯一对象、合并加载、使用期、共享访问准入与安全快照
│   ├── txn/                # Procedure、记录锁、Undo 与创建/删除的提交回滚
│   └── writeback/          # 提交后待写登记、失败保留与最终写回
├── runtime/                # 操作范围绑定、存储选择与原生 Mongo 入口
└── storage/                # 存储契约及 Redis / Mongo / Fusion 适配

txgen/
└── gen/
    ├── render_data.go      # 校验受管根能力，按缓存选择规划生成文件
    └── templates/          # 生成类型化 Find/Snapshot、私有描述与深复制

txgame/
├── gen/config.yml          # data.entityCache: false
└── server/
    ├── gen/data/
    │   └── GD*Store.go     # Store 与直接存储查询
    ├── internal/app/       # 显式组件装配
    └── services/           # 业务修改、显式保存与独立响应

txgameslg/
├── gen/config.yml          # data.entityCache: true
└── server/
    ├── gen/data/
    │   ├── GD*Store.go     # Store/查询；受管根另含私有实体描述
    │   └── entity_access.go# 包级缓存接入辅助
    ├── internal/app/       # 显式组件装配
    └── services/
        ├── slggame/        # Procedure 业务边界与独立响应
        └── slgworld/       # World 独占执行与保存策略
```

两组应用的产物分别位于 `txgame/server/gen/data/` 与 `txgameslg/server/gen/data/`。只有启用 `data.entityCache` 且存在未被 `@noEntityCache` 排除的持久根时，才生成包级缓存接入辅助文件。当前产物为 `txgameslg/server/gen/data/entity_access.go`，txgame 未生成此文件。生成接口复用现有类型描述和 Reader 选择，不公开逐 GD Cache/Descriptor 或手工登记清单。

## 安全与性能边界

- 使用期只防止回收，字段安全由同一 Procedure 记录锁保障；不增加第二套业务互斥，不自动给 Handler 开事务。
- 取消或超时不代表执行已经退出，不能据此提前释放使用权、锁或独占 World。
- 响应、事件和后台任务不得携带共享 GD/map/slice 别名。Go 公开字段无法杜绝人为逃逸；框架的入口检查不能保护开发者在锁外继续使用非法保留的指针。
- 原生 Mongo 查询/写入不自动进入 EntityCache 或 Procedure；不能用它绕过共享数据权威。
- 单实体热查询、完整 Snapshot、Procedure 修改测量的是不同工作量。当前量化结果统一维护在 [验收报告](entity-access-acceptance.md)，不再沿用旧裸访问基准作为现行性能依据。

EntityCache 统计、元数据、仅命中安全快照与尝试回收，见 [管理 API](entity-cache-management.md)。未增加管理页面、全量导出或云端容量验收。
