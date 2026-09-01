package discovery

import (
	"encoding/json"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestServiceDirectoryClonesInstanceEndpoints(t *testing.T) {
	endpoints := map[string]string{
		"agent.tcp":       "10.0.0.7:9000",
		"agent.websocket": "10.0.0.7:9001",
	}
	directory := NewServiceDirectory()
	if err := directory.Replace([]Instance{{
		ID:        "agent-a",
		Services:  []string{"agent"},
		Endpoint:  "10.0.0.7:9100",
		Endpoints: endpoints,
		Status:    StatusRunning,
	}}); err != nil {
		t.Fatalf("Replace returned error: %v", err)
	}
	endpoints["agent.tcp"] = "tampered-input"

	instance, ok := directory.NextInstance("agent")
	if !ok {
		t.Fatal("NextInstance did not find agent")
	}
	if got := instance.Endpoints["agent.tcp"]; got != "10.0.0.7:9000" {
		t.Fatalf("agent.tcp = %q, want 10.0.0.7:9000", got)
	}
	instance.Endpoints["agent.websocket"] = "tampered-result"

	stored, ok := directory.LookupInstance("agent-a")
	if !ok {
		t.Fatal("LookupInstance did not find agent-a")
	}
	if got := stored.Endpoints["agent.websocket"]; got != "10.0.0.7:9001" {
		t.Fatalf("agent.websocket = %q, want 10.0.0.7:9001", got)
	}
}

func TestEtcdInstanceDecodePreservesPublicEndpoints(t *testing.T) {
	config, err := (EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		Prefix:    "/tx/test",
		LeaseTTL:  time.Second,
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	instance := Instance{
		ID:       "agent-a",
		Services: []string{"agent"},
		Endpoint: "10.0.0.7:9100",
		Endpoints: map[string]string{
			"agent.tcp":       "10.0.0.7:9000",
			"agent.websocket": "10.0.0.7:9001",
		},
		Status: StatusRunning,
	}
	payload, err := json.Marshal(instance)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	registry := &EtcdRegistry{config: config}

	decoded, err := registry.decodeInstance(&mvccpb.KeyValue{
		Key:   []byte(config.Prefix + "/agent-a"),
		Value: payload,
	})
	if err != nil {
		t.Fatalf("decodeInstance returned error: %v", err)
	}
	if decoded.Endpoints["agent.tcp"] != "10.0.0.7:9000" || decoded.Endpoints["agent.websocket"] != "10.0.0.7:9001" {
		t.Fatalf("Endpoints = %#v", decoded.Endpoints)
	}
}
