package actorhost

import (
	"encoding/base64"
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
