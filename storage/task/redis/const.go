package redis

import "time"

const (
	defaultPrefix       = "tx:task"
	defaultBucketCount  = 64
	defaultLeaseTimeout = 30 * time.Second
)

const (
	scriptResultOK     = "ok"
	scriptResultStale  = "stale"
	scriptResultLeased = "leased"
)
