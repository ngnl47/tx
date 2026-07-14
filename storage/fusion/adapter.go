package fusion

import (
	"context"

	"github.com/ngnl47/tx/storage"
)

type Adapter struct {
	primary   storage.StoreAdapter
	secondary storage.StoreAdapter
}

func NewAdapter(primary, secondary storage.StoreAdapter) *Adapter {
	return &Adapter{primary: primary, secondary: secondary}
}

func (a *Adapter) Save(ctx context.Context, record storage.Record) error {
	if err := a.primary.Save(ctx, record); err != nil {
		return err
	}
	return a.secondary.Save(ctx, record)
}

func (a *Adapter) Load(ctx context.Context, key storage.RecordKey) (storage.Record, error) {
	return a.primary.Load(ctx, key)
}

func (a *Adapter) Delete(ctx context.Context, key storage.RecordKey) error {
	if err := a.primary.Delete(ctx, key); err != nil {
		return err
	}
	return a.secondary.Delete(ctx, key)
}
