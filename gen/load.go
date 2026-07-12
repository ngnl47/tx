package gen

import (
	"fmt"

	"github.com/ngnl47/gobricks/astscan"
	"github.com/ngnl47/gobricks/meta"
	"github.com/ngnl47/gobricks/storage"
)

func LoadTagsAndScan(sourceDir string) (*Model, error) {
	packageSet, err := astscan.ScanDir(sourceDir, astscan.Options{})
	if err != nil {
		return nil, err
	}
	registry, err := storage.ResolveMeta(packageSet.TypeSpecs(), packageSet)
	if err != nil {
		return nil, err
	}
	return NewModel(packageSet, registry), nil
}

func NewModel(types *astscan.PackageSet, registry *storage.Registry) *Model {
	model := &Model{}
	for _, typeName := range registry.TypeNames() {
		cfg, _ := registry.Lookup(typeName)
		metaSpec, ok := types.Lookup(meta.TypeRef{FullName: typeName})
		if !ok {
			metaSpec = meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: typeName}}}
		}
		model.Types = append(model.Types, TypeModel{
			TypeName: typeName,
			Meta:     metaSpec,
			Config:   cfg,
		})
	}
	return model
}

func RenderMetadataBytes(model *Model) ([]byte, error) {
	text, err := RenderMetadata(model)
	if err != nil {
		return nil, fmt.Errorf("gen: render metadata: %w", err)
	}
	return []byte(text), nil
}
