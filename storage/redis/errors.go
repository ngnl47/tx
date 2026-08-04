package redis

import "errors"

var ErrClientNotConfigured = errors.New("redis: client is not configured")
