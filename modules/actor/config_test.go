package actor

import (
	"testing"
	"time"
)

func TestConfigNormalizesIdlePassivationWithoutSharingCallerMap(t *testing.T) {
	input := map[string]time.Duration{"role": 5 * time.Minute}
	config, err := (Config{
		ClusterName:     "test",
		Host:            "127.0.0.1",
		IdlePassivation: input,
		Etcd: EtcdConfig{
			Endpoints: []string{"127.0.0.1:2379"},
		},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	input["role"] = time.Second
	if got := config.IdlePassivation["role"]; got != 5*time.Minute {
		t.Fatalf("normalized role idle passivation = %v, want 5m", got)
	}
}

func TestConfigRejectsInvalidIdlePassivation(t *testing.T) {
	_, err := (Config{
		ClusterName:     "test",
		Host:            "127.0.0.1",
		IdlePassivation: map[string]time.Duration{"role": 0},
		Etcd: EtcdConfig{
			Endpoints: []string{"127.0.0.1:2379"},
		},
	}).Normalize()
	if err == nil {
		t.Fatal("Normalize accepted zero role idle passivation")
	}
}
