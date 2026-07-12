package schema

import "errors"

var (
	ErrUnknownType     = errors.New("schema: unknown type")
	ErrUnknownField    = errors.New("schema: unknown field")
	ErrUnsupportedMode = errors.New("schema: unsupported storage mode")
	ErrMissingConfig   = errors.New("schema: missing config")
	ErrConflict        = errors.New("schema: conflict")
)
