package fusion

import (
	"context"
	"errors"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// BackfillDocumentWithOptions 创建尚未存在的 Fusion 根记录。
// 与回源共用记录锁，检查冷数据后以 Redis 条件写防止覆盖；成功登记既有延迟写回。
func (s Strategy) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, document map[string]any, options storage.ApplyOptions) (inserted bool, err error) {
	if document == nil {
		return false, storage.ErrNilDocument
	}
	if err = storage.ValidateRecordKey(spec, key); err != nil {
		return false, err
	}
	if err = s.Validate(); err != nil {
		return false, err
	}
	ttl, err := (Config{DirtyTTL: s.DirtyTTL}).validateDirtyTTL()
	if err != nil {
		return false, err
	}
	if err = validateDirtyTTLForType(ttl, spec); err != nil {
		return false, err
	}
	lock, err := s.redisLocker().LockRecord(ctx, spec, key, storage.LockOptions{TTL: s.loadLockTTL(), Wait: s.loadLockWait()})
	if err != nil {
		return false, err
	}
	defer func() {
		var cleanupErr error
		releaseRecordLock(ctx, lock, &cleanupErr)
		// 已确认的插入不能因附带解锁失败退回首次创建；锁仍受既有 TTL 约束。
		if inserted && err == nil {
			if cleanupErr != nil {
				txlog.WarnContext(ctx, "fusion creation succeeded but lock cleanup failed", "err", cleanupErr)
			}
			return
		}
		err = errors.Join(err, cleanupErr)
	}()
	_, exists, err := loadFusionDocument(ctx, s.MongoLoader, "mongo", spec, key)
	if err != nil || exists {
		return false, err
	}
	options.TTL = ttl
	inserted, err = s.redisBackfiller().BackfillDocumentWithOptions(ctx, spec, key, document, options)
	if err == nil && inserted {
		s.DelaySubmitter.Submit(spec, key)
	}
	return inserted, err
}
