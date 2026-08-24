package mongo

import (
	"fmt"
	"strings"

	"github.com/ngnl47/tx/meta"
)

// IndexKey 描述已经解析为 BSON 路径的一段 Mongo 索引键。
type IndexKey struct {
	Path  string
	Order string
}

// IndexWeight 描述已经解析为 BSON 路径的文本索引权重。
type IndexWeight struct {
	Path   string
	Weight int
}

// IndexModel 是交给具体 Mongo driver 的后端无关索引定义。
type IndexModel struct {
	Name        string
	Keys        []IndexKey
	Unique      bool
	Weights     []IndexWeight
	ExpireAfter *int64
}

// BuildIndexModels 按字段声明顺序聚合同一个类型的命名和匿名索引。
func BuildIndexModels(spec meta.TypeSpec) ([]IndexModel, error) {
	models := make([]IndexModel, 0)
	positions := make(map[string]int)
	for _, field := range spec.Type.Fields {
		fieldSpec, ok := spec.Fields[field.Name]
		if !ok || fieldSpec.Mongo == nil {
			continue
		}
		for _, index := range fieldSpec.Mongo.Indexes {
			model, err := resolveIndexModel(spec, field.Name, index)
			if err != nil {
				return nil, err
			}
			if len(model.Keys) == 1 && model.Keys[0].Path == "_id" {
				continue
			}
			if model.Name == "" {
				model.Name = derivedIndexName(model.Keys)
			}
			if position, exists := positions[model.Name]; exists {
				if err := mergeIndexModel(&models[position], model); err != nil {
					return nil, err
				}
				continue
			}
			positions[model.Name] = len(models)
			models = append(models, model)
		}
	}
	return models, nil
}

func resolveIndexModel(spec meta.TypeSpec, fieldName string, index meta.IndexSpec) (IndexModel, error) {
	if len(index.Keys) == 0 {
		return IndexModel{}, fmt.Errorf("mongo: field %s index %q has no keys", fieldName, index.Name)
	}
	model := IndexModel{Name: index.Name, Unique: index.Unique}
	if index.ExpireAfter != nil {
		expireAfter := *index.ExpireAfter
		model.ExpireAfter = &expireAfter
	}
	for _, key := range index.Keys {
		path := resolveIndexPath(spec, fieldName, key.Path)
		if path == "" || key.Order == "" {
			return IndexModel{}, fmt.Errorf("mongo: field %s index %q has empty key path or order", fieldName, index.Name)
		}
		model.Keys = append(model.Keys, IndexKey{Path: path, Order: key.Order})
	}
	for _, weight := range index.Weights {
		path := resolveIndexPath(spec, fieldName, weight.Path)
		if path == "" {
			return IndexModel{}, fmt.Errorf("mongo: field %s index %q has empty weight path", fieldName, index.Name)
		}
		model.Weights = append(model.Weights, IndexWeight{Path: path, Weight: weight.Weight})
	}
	return model, nil
}

func resolveIndexPath(spec meta.TypeSpec, fieldName, path string) string {
	fieldPath := spec.BSONName(fieldName)
	if path == "" {
		return fieldPath
	}
	if strings.HasPrefix(path, ".") {
		return fieldPath + path
	}
	return path
}

func derivedIndexName(keys []IndexKey) string {
	parts := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		parts = append(parts, key.Path, key.Order)
	}
	return strings.Join(parts, "_")
}

func mergeIndexModel(target *IndexModel, source IndexModel) error {
	if target.ExpireAfter != nil && source.ExpireAfter != nil && *target.ExpireAfter != *source.ExpireAfter {
		return fmt.Errorf("mongo: index %q has conflicting expireAfter values %d and %d", target.Name, *target.ExpireAfter, *source.ExpireAfter)
	}
	if target.ExpireAfter == nil && source.ExpireAfter != nil {
		expireAfter := *source.ExpireAfter
		target.ExpireAfter = &expireAfter
	}
	target.Unique = target.Unique || source.Unique
	for _, key := range source.Keys {
		for _, existing := range target.Keys {
			if existing.Path == key.Path {
				return fmt.Errorf("mongo: index %q declares key %q more than once", target.Name, key.Path)
			}
		}
		target.Keys = append(target.Keys, key)
	}
	for _, weight := range source.Weights {
		for _, existing := range target.Weights {
			if existing.Path == weight.Path && existing.Weight != weight.Weight {
				return fmt.Errorf("mongo: index %q has conflicting weights for %q", target.Name, weight.Path)
			}
		}
		target.Weights = append(target.Weights, weight)
	}
	return nil
}
