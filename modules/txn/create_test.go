package txn

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/change"
)

type creationStore struct{ recorder change.UndoRecorder }

func (s *creationStore) SetUndoRecorder(r change.UndoRecorder) { s.recorder = r }

type creationValue struct {
	id    string
	store creationStore
}

func (v *creationValue) StoreTarget() StoreTarget {
	return NewStoreTarget(NewLockey("create", 0, v.id), &v.store)
}

type creationReservation struct{ committed, aborted bool }

func (r *creationReservation) Commit()         { r.committed = true }
func (r *creationReservation) Abort()          { r.aborted = true }
func (r *creationReservation) Validate() error { return nil }

type creationParticipant struct{ resources []*creationReservation }

func (p *creationParticipant) Prepare(context.Context, []StoreTarget) (CommitReservation, error) {
	return emptyReservation{}, nil
}
func (p *creationParticipant) PrepareCreate(context.Context, StoreTarget, any) (CreationReservation, error) {
	r := &creationReservation{}
	p.resources = append(p.resources, r)
	return r, nil
}

type emptyReservation struct{}

func (emptyReservation) Commit([]StoreTarget) {}
func (emptyReservation) Abort()               {}

type createProcedure struct {
	root creationValue
	fn   func(*Tx) error
}

func (p *createProcedure) Locks(context.Context) (Lockeys, error) { return Locks(&p.root), nil }
func (p *createProcedure) Process(tx *Tx) error                   { return p.fn(tx) }

func TestCreateSavepointAndExpiredContext(t *testing.T) {
	participant := &creationParticipant{}
	runtime := NewRuntime()
	if err := runtime.BindCommitParticipant(participant); err != nil {
		t.Fatal(err)
	}
	var retained context.Context
	childErr := errors.New("child")
	p := &createProcedure{root: creationValue{id: "root"}}
	p.fn = func(tx *Tx) error {
		retained = tx.Context()
		if err := tx.Do(func(tx *Tx) error {
			if err := Create(tx.Context(), &creationValue{id: "child"}); err != nil {
				return err
			}
			return childErr
		}); !errors.Is(err, childErr) {
			t.Fatal(err)
		}
		if !participant.resources[0].aborted {
			t.Fatal("child reservation leaked")
		}
		return Create(tx.Context(), &creationValue{id: "kept"})
	}
	if err := runtime.Call(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if participant.resources[0].committed || !participant.resources[1].committed {
		t.Fatal("wrong creation published")
	}
	if err := Create(retained, &creationValue{id: "late"}); !errors.Is(err, ErrInactive) {
		t.Fatalf("expired context: %v", err)
	}
}

func TestCreateIdentityChangeRollsBack(t *testing.T) {
	participant := &creationParticipant{}
	runtime := NewRuntime()
	_ = runtime.BindCommitParticipant(participant)
	value := &creationValue{id: "before"}
	p := &createProcedure{root: creationValue{id: "root"}, fn: func(tx *Tx) error {
		if err := Create(tx.Context(), value); err != nil {
			return err
		}
		value.id = "after"
		return nil
	}}
	if err := runtime.Call(context.Background(), p); err == nil {
		t.Fatal("identity mutation accepted")
	}
	if !participant.resources[0].aborted || participant.resources[0].committed {
		t.Fatal("invalid creation published")
	}
}

type createOnlyProcedure struct{ value *creationValue }

func (p createOnlyProcedure) Locks(context.Context) (Lockeys, error) { return nil, nil }
func (p createOnlyProcedure) Process(tx *Tx) error                   { return Create(tx.Context(), p.value) }
func TestManagedProcedureCanCreateWithoutExistingRoot(t *testing.T) {
	participant := &creationParticipant{}
	runtime := NewRuntime()
	_ = runtime.BindCommitParticipant(participant)
	if err := runtime.Call(context.Background(), createOnlyProcedure{&creationValue{id: "only"}}); err != nil {
		t.Fatal(err)
	}
	if !participant.resources[0].committed {
		t.Fatal("new root was not committed")
	}
}
