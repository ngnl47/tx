package mongo

import "errors"

var ErrClientNotConfigured = errors.New("mongo: client is not configured")
