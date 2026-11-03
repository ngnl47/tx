package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/txn"
	txruntime "github.com/ngnl47/tx/runtime"
)

func TestRuntimeSaveReturnsStableProcedureError(t *testing.T) {
	root := &managedRoot{}
	var expired context.Context
	if err := txn.Run(t.Context(), []txn.StoreTarget{root.StoreTarget()}, func(tx *txn.Tx) error {
		expired = tx.Context()
		if err := txruntime.Save(tx.Context(), root); !errors.Is(err, txruntime.ErrProcedureWrite) {
			t.Fatalf("事务内直接保存没有返回稳定错误: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := txruntime.Save(expired, root); !errors.Is(err, txn.ErrInactive) {
		t.Fatalf("结束后的事务上下文返回 %v，期望 %v", err, txn.ErrInactive)
	}
}
