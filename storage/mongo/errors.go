package mongo

import "errors"

var ErrUnsupportedClient = errors.New("mongo: adapter client does not implement required capability")
