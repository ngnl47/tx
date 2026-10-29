package fusion

import (
	"context"
	"fmt"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// ApplyDelete 同步完成删除，失败保留 Redis 删除意图并由既有延迟任务重试。
// 与回源、创建及延迟落盘共用记录锁，不承诺跨后端原子事务。
func (s Strategy) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (err error) {
	if err = storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	marker, ok := s.RedisApplier.(storage.DeletionMarker)
	if !ok {
		return fmt.Errorf("fusion: deletion marker is not configured")
	}
	if _, ok := s.MongoLoader.(storage.DeleteApplier); !ok {
		return fmt.Errorf("fusion: mongo deletion is not configured")
	}
	if _, ok := s.RedisApplier.(storage.DeleteApplier); !ok {
		return fmt.Errorf("fusion: redis deletion is not configured")
	}
	if err = s.Validate(); err != nil {
		return err
	}
	lock, err := s.redisLocker().LockRecord(ctx, spec, key, storage.LockOptions{TTL: s.loadLockTTL(), Wait: s.loadLockWait()})
	if err != nil {
		return err
	}
	defer func() { releaseRecordLock(ctx, lock, &err) }()
	if err = marker.MarkDeletion(ctx, spec, key); err != nil {
		return err
	}
	s.DelaySubmitter.Submit(spec, key)
	return finishDeletion(ctx, spec, key, s.MongoLoader, s.RedisApplier)
}

func finishDeletion(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, mongo, redis any) error {
	cold, ok := mongo.(storage.DeleteApplier)
	if !ok {
		return fmt.Errorf("fusion: mongo deletion is not configured")
	}
	hot, ok := redis.(storage.DeleteApplier)
	if !ok {
		return fmt.Errorf("fusion: redis deletion is not configured")
	}
	if err := cold.ApplyDelete(ctx, spec, key); err != nil {
		return err
	}
	return hot.ApplyDelete(ctx, spec, key)
}
