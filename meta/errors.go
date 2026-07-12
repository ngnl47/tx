package meta

import "errors"

var (
	ErrDuplicateType       = errors.New("meta: duplicate type")
	ErrRegistryFrozen      = errors.New("meta: registry frozen")
	ErrDuplicateFieldAlias = errors.New("meta: duplicate field alias")
	ErrInvalidTypeRef      = errors.New("meta: invalid type ref")
)
