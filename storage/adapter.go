package storage

import (
	"context"
	"errors"
)

var ErrAdapterMethodMissing = errors.New("storage: adapter method missing")

type RecordKey struct {
	TypeName string
	ID       any
	ShardKey any
}

type Record struct {
	TypeName string
	Values   map[string]any
}

type StoreAdapter interface {
	Save(ctx context.Context, record Record) error
	Load(ctx context.Context, key RecordKey) (Record, error)
	Delete(ctx context.Context, key RecordKey) error
}

type AdapterFuncs struct {
	SaveFunc   func(context.Context, Record) error
	LoadFunc   func(context.Context, RecordKey) (Record, error)
	DeleteFunc func(context.Context, RecordKey) error
}

func (a AdapterFuncs) Save(ctx context.Context, record Record) error {
	if a.SaveFunc == nil {
		return ErrAdapterMethodMissing
	}
	return a.SaveFunc(ctx, record)
}

func (a AdapterFuncs) Load(ctx context.Context, key RecordKey) (Record, error) {
	if a.LoadFunc == nil {
		return Record{}, ErrAdapterMethodMissing
	}
	return a.LoadFunc(ctx, key)
}

func (a AdapterFuncs) Delete(ctx context.Context, key RecordKey) error {
	if a.DeleteFunc == nil {
		return ErrAdapterMethodMissing
	}
	return a.DeleteFunc(ctx, key)
}
