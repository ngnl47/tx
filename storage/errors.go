package storage

import "errors"

var (
	ErrRegistryFrozen = errors.New("storage: registry frozen")
	ErrDuplicateType  = errors.New("storage: duplicate type")
	ErrMissingValue   = errors.New("storage: missing value")
	ErrNotExists      = errors.New("storage: not exists")
)
