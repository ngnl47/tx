package msgq

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

type admissionTestStore struct {
	testSessionStore
	mu      sync.Mutex
	records map[int64]AdmissionRecord
}

func (s *admissionTestStore) LoadAdmission(_ context.Context, roleID int64) (AdmissionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[roleID], nil
}

func (s *admissionTestStore) CompareAdmission(_ context.Context, roleID int64, expected, next AdmissionRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.records[roleID], expected) {
		return false, nil
	}
	if s.records == nil {
		s.records = map[int64]AdmissionRecord{}
	}
	s.records[roleID] = next
	return true, nil
}

func admissionServer(t *testing.T) *Server {
	t.Helper()
	s, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &admissionTestStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
