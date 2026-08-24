package storage

import (
	"fmt"
	"time"
)

// ValidateMillisecondDuration 校验需要转换为毫秒整数的正数时长。
// 调用方不得依赖静默截断或向上取整。
func ValidateMillisecondDuration(value time.Duration) error {
	if value <= 0 {
		return fmt.Errorf("duration must be greater than 0")
	}
	if value < time.Millisecond {
		return fmt.Errorf("duration must be at least 1ms")
	}
	if value%time.Millisecond != 0 {
		return fmt.Errorf("duration must use whole milliseconds")
	}
	return nil
}

// ValidateExpiringTTL 校验需要写入后端过期命令的正数 TTL。
func ValidateExpiringTTL(ttl time.Duration) error {
	if err := ValidateMillisecondDuration(ttl); err != nil {
		return fmt.Errorf("TTL: %w", err)
	}
	return nil
}
