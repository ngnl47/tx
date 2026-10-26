package actorhost

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// PartitionSlot 是普通实体 v1 的持久化分区布局，共 1024 个分区。
// 输入为已校验身份，保留原始 Key 字节；布局改变必须经过归属迁移，不能热改。
// 世界等独立槽位通过组合根提供自定义映射，不使用此函数。
func PartitionSlot(id Identity) string {
	return "actor-v1." + base64.RawURLEncoding.EncodeToString([]byte(id.Type)) + ".p" + strconv.FormatUint(xxhash.Sum64String(id.ID)%1024, 10)
}

// SlotMapping 将身份映射到稳定业务槽位；零值使用普通实体的 1024 分区。
// 映射须在所有参与路由和加载的实例上一致，不能依赖会话状态或当前成员数。
// 自定义结果不回退默认规则；空槽位交由归属入口拒绝。
type SlotMapping func(Identity) string

// Resolve 解析唯一槽位规则，供路由、首次分配与权威加载校验共用。
func (mapping SlotMapping) Resolve(id Identity) string {
	if mapping != nil {
		return mapping(id)
	}
	return PartitionSlot(id)
}

// SlotResolver 支持首次归属查询；解析错误必须阻断寻址和加载，不能回退分区。
type SlotResolver func(context.Context, Identity) (string, error)

// Resolve 对默认布局与自定义解析统一执行取消和空结果检查。
func (resolve SlotResolver) Resolve(ctx context.Context, id Identity) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if resolve == nil {
		return PartitionSlot(id), nil
	}
	slot, err := resolve(ctx, id)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if slot == "" {
		return "", fmt.Errorf("actor: empty slot for %v", id)
	}
	return slot, nil
}

// Resolver 把纯计算映射接入同一个解析管线。
func (mapping SlotMapping) Resolver() SlotResolver {
	if mapping == nil {
		return nil
	}
	return func(_ context.Context, id Identity) (string, error) { return mapping(id), nil }
}

// GroupResolver 返回稳定业务分组 ID；不选择实例或编码槽位。
type GroupResolver func(context.Context, Identity) (string, error)

// PlacementSlot 为运维入口提供与运行时一致的分组槽位编码。
// 服务池和策略隔离命名空间，Actor Type 不参与编码。
func PlacementSlot(service, policy, group string) string {
	encode := base64.RawURLEncoding.EncodeToString
	return "actor-group-v1." + encode([]byte(service)) + "." + encode([]byte(policy)) + "." + encode([]byte(group))
}

// PlacementName 返回类型引用的服务内放置策略。
func PlacementName(k Type) string { return k.placement }

// WithPlacement 引用目标服务声明的分组策略，由框架托管查询和缓存。
func WithPlacement(name string) TypeOption {
	return typeOption(func(k *Type) error {
		if name == "" || k.placement != "" || k.slot != nil {
			return ErrInvalidType
		}
		k.placement = name
		return nil
	})
}
