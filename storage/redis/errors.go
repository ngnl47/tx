package redis

import "errors"

var ErrClientNotConfigured = errors.New("redis: client is not configured")

var (
	ErrHandoffFull    = errors.New("redis: fusion handoff slot is full")
	ErrInvalidHandoff = errors.New("redis: invalid fusion handoff")
)
