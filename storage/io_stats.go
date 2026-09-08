package storage

import "time"

// IOStat 是存储运行时的累计驱动统计，不包含端点、key、命令参数或业务身份。
// Calls 是完成调用数；Commands 对 Redis pipeline 为批内命令数，其他调用为 1。
// TotalDuration/MaxDuration 对 pipeline 记录整批耗时，不摊算单条命令延迟。
type IOStat struct {
	Backend       string        `json:"backend"`
	Operation     string        `json:"operation"`
	Calls         uint64        `json:"calls"`
	Commands      uint64        `json:"commands"`
	Errors        uint64        `json:"errors"`
	TotalDuration time.Duration `json:"totalDurationNs"`
	MaxDuration   time.Duration `json:"maxDurationNs"`
}
