package tests

import (
	"testing"

	"github.com/ngnl47/tx/storage"
)

func TestValidateDocumentResultRejectsInconsistentExistsState(t *testing.T) {
	if err := storage.ValidateDocumentResult(nil, true); err == nil {
		t.Fatal("ValidateDocumentResult(nil, true) error = nil, want existing document without data error")
	}
	if err := storage.ValidateDocumentResult(map[string]any{"name": "alice"}, false); err == nil {
		t.Fatal("ValidateDocumentResult(document, false) error = nil, want missing document with data error")
	}
}

func TestValidateDocumentResultsRejectsInconsistentBatchRow(t *testing.T) {
	err := storage.ValidateDocumentResults(
		[]map[string]any{{"name": "alice"}, nil},
		[]bool{true, true},
	)
	if err == nil {
		t.Fatal("ValidateDocumentResults error = nil, want existing document without data error")
	}
}
