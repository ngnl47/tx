package meta

import "errors"

var (
	ErrDuplicateType       = errors.New("meta: duplicate type")
	ErrRegistryFrozen      = errors.New("meta: registry frozen")
	ErrDuplicateFieldAlias = errors.New("meta: duplicate field alias")
	ErrInvalidTypeRef      = errors.New("meta: invalid type ref")
	ErrUnknownTag          = errors.New("meta: unknown tag")
	ErrUnknownParam        = errors.New("meta: unknown param")
	ErrUnsupportedMode     = errors.New("meta: unsupported storage mode")
	ErrMissingConfig       = errors.New("meta: missing config")
	ErrUnknownField        = errors.New("meta: unknown field")
)
