package tests

import (
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
)

var _ storage.ChangeApplier = (*txredis.Adapter)(nil)
