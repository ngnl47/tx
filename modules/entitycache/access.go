package entitycache

import (
	"context"

	"github.com/ngnl47/tx/storage"
)

// Access 只提供实体访问与管理操作，生命周期由持有 Runtime 的宿主控制。
type Access struct{ runtime *Runtime }

// Access 返回同一缓存的受限视图，不创建缓存或后台任务。
func (r *Runtime) Access() *Access { return &r.access }

// Find 在有效 Procedure 内取得共享可变对象，可能触发存储加载。
// 使用权和记录锁保留到事务结束；不得将对象及其子引用带出事务继续读写。
// 业务通常通过生成的 FindGD* 进入此方法，不自行构造 Descriptor。
func (a *Access) Find(ctx context.Context, descriptor *Descriptor, key storage.RecordKey) (any, bool, error) {
	return a.runtime.Find(ctx, descriptor, key)
}

// Snapshot 返回独立深复制，未缓存时可以加载存储；结果不能用于保存或删除原共享实体。
// 普通调用在记录锁内复制；有效 Procedure 内复用事务锁，可读到本事务未提交的修改。
// 每条快照独立一致，多次调用不保证跨记录同一时刻一致。
func (a *Access) Snapshot(ctx context.Context, descriptor *Descriptor, key storage.RecordKey) (any, bool, error) {
	return a.runtime.Snapshot(ctx, descriptor, key)
}

// Stats 只观察框架规模和配置，不加载或读取 GD、不刷新淘汰热度。
// Entries 包含过渡条目，各指标不保证同一时刻一致；MaximumSize 是软回收预算。
func (a *Access) Stats(ctx context.Context) (Stats, error) { return a.runtime.Stats(ctx) }

// List 返回有界元数据和驻留引用，不读业务字段、不触发加载，不提供排序或稳定分页。
// 筛选最坏扫描全部条目，只适合低频诊断；EntryRef 不持有使用权，后续操作会重新校验。
func (a *Access) List(ctx context.Context, options ListOptions) (EntryList, error) {
	return a.runtime.List(ctx, options)
}

// SnapshotCached 仅复制 EntryRef 指定的缓存驻留对象，不加载存储或刷新淘汰热度。
// 必须在 Procedure 外调用；引用失效、未缓存或过渡状态返回错误，不等待后台加载。
// 复制在记录锁内完成，结果可在锁外序列化；取消不强制中止已开始的同步复制。
func (a *Access) SnapshotCached(ctx context.Context, ref EntryRef) (any, error) {
	return a.runtime.SnapshotCached(ctx, ref)
}

// TryRelease 在 Procedure 外尝试回收指定驻留对象，不刷盘、不删记录、不卸载 Actor。
// 使用中或持久化边界不允许释放时返回 Busy 或 Retained；错误仍保留对象。
// 不启动后台重试；Released 只表示本次驻留已移除，后续业务仍可重新加载。
func (a *Access) TryRelease(ctx context.Context, ref EntryRef) (ReleaseResult, error) {
	return a.runtime.TryRelease(ctx, ref)
}
