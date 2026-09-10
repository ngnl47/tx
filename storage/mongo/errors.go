package mongo

import "errors"

var ErrClientNotConfigured = errors.New("mongo: client is not configured")

// ErrVersionConflict 表示记录已消失或版本条件不再匹配，当前条件写入没有生效。
var ErrVersionConflict = errors.New("mongo: document version conflict")
