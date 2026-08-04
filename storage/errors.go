package storage

import "errors"

var (
	ErrRegistryFrozen   = errors.New("storage: registry frozen")
	ErrDuplicateType    = errors.New("storage: duplicate type")
	ErrMissingValue     = errors.New("storage: missing value")
	ErrNotExists        = errors.New("storage: not exists")
	ErrPathTypeMismatch = errors.New("storage: document path type mismatch")
	ErrNilDeltaApplier  = errors.New("storage: change applier is nil")
	ErrNilDeltaStore    = errors.New("storage: delta store is nil")
	ErrReadOnlyStore    = errors.New("storage: view store cannot be persisted")
)
