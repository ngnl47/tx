package ant

import (
	"testing"

	"github.com/ngnl47/tx/modules/txn"
)

func TestStatefulConfigurationIsExplicit(t *testing.T) {
	config := Config{
		Runtime: RuntimeConfig{
			Transaction: TransactionConfig{Module: txn.Config{}},
		},
	}

	if config.toFramework().Runtime.Transaction.Module != config.Runtime.Transaction.Module {
		t.Fatal("transaction configuration was not preserved")
	}
}
