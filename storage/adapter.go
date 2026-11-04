// Package storage 定义 GD 文档读写、变更应用和条件写的后端无关契约。
//
// RecordKey 表达完整存储身份；DocumentLoader 读取文档，ChangeApplier 应用规范化变更，
// DocumentBackfiller 与 VersionedDocumentReplacer 分别提供条件创建和版本替换。
// Create/Save 是显式接收 Writer 的低层入口，不建立事务、缓存或连接。
//
// 普通应用通过生成查询与 tx 的 runtime 存取入口选择已声明能力；
// Redis、Mongo 适配位于子包，Fusion 组合热写与延迟落盘，storage/runtime 由宿主装配共享资源。
// 成功确认遵循具体后端契约，不提供跨后端 ACID；不得旁路写入受管的同一实体。
package storage

// RecordKey 保存记录 ID 与存储路由键，不等同于 Actor ID 或 Procedure 的 Lockey。
// 键的合法类型由 TypeSpec 与 ValidateRecordKey 决定。
type RecordKey struct {
	ID       any
	ShardKey any
}
