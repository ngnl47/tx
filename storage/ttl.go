package storage

import (
	"fmt"
	"time"
)

// ValidateExpiringTTL 校验需要写入后端过期命令的正数 TTL。
// Redis 以毫秒作为统一精度，调用方不得依赖静默截断或向上取整。
func ValidateExpiringTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("TTL must be greater than 0")
	}
	if ttl < time.Millisecond {
		return fmt.Errorf("TTL must be at least 1ms")
	}
	if ttl%time.Millisecond != 0 {
		return fmt.Errorf("TTL must use whole milliseconds")
	}
	return nil
}
