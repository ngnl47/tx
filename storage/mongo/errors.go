package mongo

import "errors"

var ErrClientNotConfigured = errors.New("mongo: client is not configured")

// ErrFenceRejected 表示记录存在性或代际条件不再匹配，当前写入没有生效。
var ErrFenceRejected = errors.New("mongo: write fence rejected")
