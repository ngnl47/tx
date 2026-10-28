package taskqueue

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// taskJSON 仅取消 Task 的 JSON 方法，保留原有信封字段。
type taskJSON Task

// MarshalJSON 在持久任务边界将不透明 Claims 字节编码为 Base64，不改动业务身份对象。
func (task Task) MarshalJSON() ([]byte, error) {
	wire := taskJSON(task)
	if task.Delivery.Identity != nil {
		identity := *task.Delivery.Identity
		identity.Claims = base64.StdEncoding.EncodeToString([]byte(identity.Claims))
		wire.Delivery.Identity = &identity
	}
	return json.Marshal(wire)
}

// UnmarshalJSON 恢复 Claims 原始字节；格式损坏直接返回错误，不能伪装成有效业务身份。
func (task *Task) UnmarshalJSON(encoded []byte) error {
	var wire taskJSON
	if err := json.Unmarshal(encoded, &wire); err != nil {
		return err
	}
	if wire.Delivery.Identity != nil {
		claims, err := base64.StdEncoding.DecodeString(wire.Delivery.Identity.Claims)
		if err != nil {
			return fmt.Errorf("task: decode identity claims: %w", err)
		}
		wire.Delivery.Identity.Claims = string(claims)
	}
	*task = Task(wire)
	return nil
}
