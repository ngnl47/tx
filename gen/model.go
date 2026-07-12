package gen

import (
	"encoding/json"

	"github.com/ngnl47/gobricks/meta"
	"github.com/ngnl47/gobricks/storage"
)

type Model struct {
	Types []TypeModel `json:"types"`
}

type TypeModel struct {
	TypeName string             `json:"typeName"`
	Meta     meta.TypeSpec      `json:"meta"`
	Config   storage.TypeConfig `json:"config"`
}

func RenderMetadata(model *Model) (string, error) {
	data, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data) + "\n", nil
}
