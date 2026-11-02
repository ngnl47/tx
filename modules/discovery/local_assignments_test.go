package discovery

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 认领不依赖 Watch 及时回显，且不能在没有运维确认时替换其他 owner。
func TestLocalAssignmentsClaimAndRefresh(t *testing.T) {
	registry, _, _ := newIdentityIntegrationRegistry(t, "local-claim", time.Second)
	putReleaseMember(t, registry, "local", StatusStarting)
	putReleaseMember(t, registry, "old", StatusRunning)
	registration := newRuntimeRegistrationFake()
	registration.instance = Instance{ID: "local"}
	r, err := newRuntime(&runtimeRegistryFake{}, registration, registry.Slots())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if err := r.slotDirectory.Replace(nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, owner, previous string
		generation            uint64
		want                  error
	}{
		{name: "new", generation: 1},
		{name: "already-local", owner: "local", generation: 3},
		{name: "released", generation: 5},
		{name: "confirmed", owner: "old", previous: "old", generation: 4},
		{name: "unconfirmed", owner: "old", generation: 4, want: ErrSlotConflict},
		{name: "wrong-confirmation", owner: "old", previous: "wrong", generation: 4, want: ErrSlotConflict},
		{name: "missing-confirmed", previous: "old", generation: 1, want: ErrSlotNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.generation > 1 {
				a := SlotAssignment{SlotID: tc.name, RuntimeInstanceID: tc.owner, Generation: tc.generation}
				if a.Released() {
					a.RuntimeInstanceID = "old"
					a.Generation--
					if err := registry.Slots().CreateSlot(t.Context(), a); err != nil {
						t.Fatal(err)
					}
					if err := registry.Slots().ReleaseSlots(t.Context(), "old"); err != nil {
						t.Fatal(err)
					}
				} else if err := registry.Slots().CreateSlot(t.Context(), a); err != nil {
					t.Fatal(err)
				}
			}
			got, err := r.LocalAssignments().Claim(t.Context(), tc.name, tc.previous)
			if tc.want != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("got %v want %v", err, tc.want)
				}
				return
			}
			wantGeneration := tc.generation
			if tc.generation > 1 && tc.owner != "local" {
				wantGeneration++
			}
			if err != nil || got.RuntimeInstanceID != "local" || got.Generation != wantGeneration {
				t.Fatalf("认领: %+v %v", got, err)
			}
			cached, err := r.Slots().ResolveSlot(t.Context(), tc.name)
			if err != nil || cached != got {
				t.Fatalf("本地缓存: %+v %v", cached, err)
			}
		})
	}
}

type unknownClaimStore struct {
	*EtcdSlotStore
	writes  int
	failure error
}

func (s *unknownClaimStore) CreateSlot(ctx context.Context, a SlotAssignment) error {
	s.writes++
	if err := s.EtcdSlotStore.CreateSlot(ctx, a); err != nil {
		return err
	}
	return s.failure
}

func TestLocalAssignmentsDoesNotRetryUnknownWrite(t *testing.T) {
	registry, _, _ := newIdentityIntegrationRegistry(t, "local-claim-unknown", time.Second)
	putReleaseMember(t, registry, "local", StatusRunning)
	registration := newRuntimeRegistrationFake()
	registration.instance = Instance{ID: "local"}
	store := &unknownClaimStore{EtcdSlotStore: registry.Slots(), failure: errors.New("unknown write result")}
	r, err := newRuntime(&runtimeRegistryFake{}, registration, store)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if err := r.slotDirectory.Replace(nil); err != nil {
		t.Fatal(err)
	}
	_, err = r.LocalAssignments().Claim(t.Context(), "slot", "")
	if !errors.Is(err, store.failure) || store.writes != 1 {
		t.Fatalf("未知结果被吞掉或重试: %v %d", err, store.writes)
	}
	if _, err := r.Slots().ResolveSlot(t.Context(), "slot"); !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("未知结果不应确认本地认领: %v", err)
	}
}
