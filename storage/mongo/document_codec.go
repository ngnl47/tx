package mongo

import (
	"bytes"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/ngnl47/tx/meta"
)

// bsonDocumentFromCanonical 将公共 JSON 字段名文档转换为 Mongo BSON 字段名文档。
func bsonDocumentFromCanonical(spec meta.TypeSpec, document map[string]any) (map[string]any, error) {
	if document == nil {
		return nil, nil
	}
	value, err := newDocumentValue(spec)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("mongo: encode canonical document: %w", err)
	}
	if err := json.Unmarshal(encoded, value); err != nil {
		return nil, fmt.Errorf("mongo: decode canonical document: %w", err)
	}
	encoded, err = bson.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("mongo: encode BSON document: %w", err)
	}
	var result map[string]any
	if err := bson.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("mongo: decode BSON document: %w", err)
	}
	return result, nil
}

// canonicalDocumentFromBSON 将 Mongo BSON 字段名文档转换为公共 JSON 字段名文档。
func canonicalDocumentFromBSON(spec meta.TypeSpec, document map[string]any, fields []string) (map[string]any, error) {
	if document == nil {
		return nil, nil
	}
	value, err := newDocumentValue(spec)
	if err != nil {
		return nil, err
	}
	encoded, err := bson.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("mongo: encode BSON document: %w", err)
	}
	if err := bson.Unmarshal(encoded, value); err != nil {
		return nil, fmt.Errorf("mongo: decode BSON document: %w", err)
	}
	encoded, err = json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("mongo: encode canonical document: %w", err)
	}
	result, err := decodeCanonicalDocument(encoded)
	if err != nil {
		return nil, err
	}
	return projectCanonicalDocument(spec, result, fields), nil
}

func newDocumentValue(spec meta.TypeSpec) (any, error) {
	if spec.DocumentFactory == nil {
		return nil, fmt.Errorf("mongo: type %q is missing generated document factory", spec.Type.Ref.FullName)
	}
	value := spec.DocumentFactory()
	if value == nil {
		return nil, fmt.Errorf("mongo: type %q document factory returned nil", spec.Type.Ref.FullName)
	}
	return value, nil
}

func decodeCanonicalDocument(encoded []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	result := map[string]any{}
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("mongo: decode canonical document: %w", err)
	}
	return result, nil
}

func projectCanonicalDocument(spec meta.TypeSpec, document map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		return document
	}
	result := make(map[string]any, len(fields))
	for _, field := range fields {
		name := spec.JSONName(field)
		if value, ok := document[name]; ok {
			result[name] = value
		}
	}
	return result
}
