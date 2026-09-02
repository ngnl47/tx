package etcd

import (
	"errors"
	"testing"
)

func TestConfigNormalizeClonesEndpoints(t *testing.T) {
	input := []string{"127.0.0.1:2379"}
	config, err := (Config{Endpoints: input}).Normalize()
	if err != nil {
		t.Fatal(err)
	}

	input[0] = "changed"
	if config.Endpoints[0] != "127.0.0.1:2379" {
		t.Fatal("endpoints were not cloned")
	}
}

func TestNewWithClientRejectsNilClient(t *testing.T) {
	if _, err := NewWithClient(nil, false); !errors.Is(err, ErrClientNil) {
		t.Fatalf("error = %v, want ErrClientNil", err)
	}
}
