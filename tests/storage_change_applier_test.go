package tests

import (
	"testing"

	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestRedisAdapterImplementsChangeApplierWithOptions(t *testing.T) {
	var _ storage.ChangeApplierWithOptions = (*txredis.Adapter)(nil)
}
