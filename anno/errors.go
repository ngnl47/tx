package anno

import "errors"

var (
	ErrUnknownTag      = errors.New("anno: unknown tag")
	ErrUnknownParam    = errors.New("anno: unknown param")
	ErrUnsupportedMode = errors.New("anno: unsupported storage mode")
	ErrMissingConfig   = errors.New("anno: missing config")
	ErrUnknownField    = errors.New("anno: unknown field")
)
