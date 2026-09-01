package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestSessionControlBroadcasterCallsEveryRunningAgent(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{ID: "agent-a", Services: []string{"agent"}, Endpoint: "agent-a:9000", Status: discovery.StatusRunning},
		{ID: "agent-b", Services: []string{"agent"}, Endpoint: "agent-b:9000", Status: discovery.StatusRunning},
	}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	dialer := &sessionControlDialerFake{clients: map[string]*sessionControlClientFake{
		"agent-a": {disconnectResult: SessionControlResult{SessionID: 99}},
		"agent-b": {disconnectResult: SessionControlResult{SessionID: 99, ConnectionID: 8, Handled: true}},
	}}
	broadcaster, err := NewSessionControlBroadcaster(directory, dialer)
	if err != nil {
		t.Fatalf("new broadcaster: %v", err)
	}

	result := broadcaster.DisconnectSession(context.Background(), 99, "duplicate_login")
	if !result.Handled() || result.Uncertain() || len(result.Instances) != 2 {
		t.Fatalf("broadcast result = %#v", result)
	}
	if result.Instances[0].InstanceID != "agent-a" || result.Instances[0].Handled {
		t.Fatalf("agent-a result = %#v", result.Instances[0])
	}
	if result.Instances[1].InstanceID != "agent-b" || !result.Instances[1].Handled || result.Instances[1].ConnectionID != 8 {
		t.Fatalf("agent-b result = %#v", result.Instances[1])
	}
	if dialer.clients["agent-a"].disconnectReason != "duplicate_login" || dialer.clients["agent-b"].disconnectReason != "duplicate_login" {
		t.Fatalf("disconnect reasons = %q/%q", dialer.clients["agent-a"].disconnectReason, dialer.clients["agent-b"].disconnectReason)
	}
}

func TestSessionControlBroadcastIsUncertainWhenNoAgentHandlesAndOneFails(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{ID: "agent-a", Services: []string{"agent"}, Endpoint: "agent-a:9000", Status: discovery.StatusRunning},
		{ID: "agent-b", Services: []string{"agent"}, Endpoint: "agent-b:9000", Status: discovery.StatusRunning},
	}); err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	dialErr := errors.New("agent-b unavailable")
	broadcaster, err := NewSessionControlBroadcaster(directory, &sessionControlDialerFake{
		clients: map[string]*sessionControlClientFake{"agent-a": {terminateResult: SessionControlResult{SessionID: 99}}},
		errors:  map[string]error{"agent-b": dialErr},
	})
	if err != nil {
		t.Fatalf("new broadcaster: %v", err)
	}

	result := broadcaster.TerminateSession(context.Background(), 99, "account_banned")
	if result.Handled() || !result.Uncertain() || len(result.Instances) != 2 || !errors.Is(result.Instances[1].Err, dialErr) {
		t.Fatalf("broadcast result = %#v", result)
	}
}

type sessionControlDialerFake struct {
	clients map[string]*sessionControlClientFake
	errors  map[string]error
}

func (dialer *sessionControlDialerFake) DialSessionControl(_ context.Context, instance discovery.Instance) (SessionControlClient, error) {
	if err := dialer.errors[instance.ID]; err != nil {
		return nil, err
	}
	return dialer.clients[instance.ID], nil
}

type sessionControlClientFake struct {
	disconnectResult SessionControlResult
	terminateResult  SessionControlResult
	disconnectReason string
	terminateReason  string
}

func (client *sessionControlClientFake) DisconnectSession(_ context.Context, _ int64, reason string) (SessionControlResult, error) {
	client.disconnectReason = reason
	return client.disconnectResult, nil
}

func (client *sessionControlClientFake) TerminateSession(_ context.Context, _ int64, reason string) (SessionControlResult, error) {
	client.terminateReason = reason
	return client.terminateResult, nil
}

func (*sessionControlClientFake) Close() error { return nil }
