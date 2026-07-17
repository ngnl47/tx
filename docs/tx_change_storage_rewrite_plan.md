# Tx Change And Storage Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a clean replacement for legacy framework data generation, change tracking, and redis/mongo/fusion persistence while keeping runtime infrastructure in `tx` and generator/integration fixtures in `txgen`.

**Architecture:** `tx` owns stable runtime libraries: annotation metadata, change tracking, and storage adapters. `txgen` owns code generation: it scans `input`, emits generated Go code into `output`, and verifies generated code against real scaffold templates. Generated business code imports `github.com/ngnl47/tx` at runtime and does not import `txgen`.

**Tech Stack:** Go 1.25.6, `go/ast`, `go/printer`, generated Go source, official MongoDB Go Driver, go-redis, existing `tx/anno`, `tx/astscan`, `tx/meta`, `tx/storage`.

## Global Constraints

- Runtime infrastructure belongs in `/Users/a47/workspace/tx`.
- Generator logic and scaffold integration tests belong in `/Users/a47/workspace/txgen`.
- `txgen/input` is read-only fixture input during tests; generated files are written to `txgen/output`.
- Generated business code may import `github.com/ngnl47/tx`, but must not import `github.com/ngnl47/txgen`.
- Do not preserve legacy framework compatibility APIs such as `Mark`, `MarkInRoot`, and `ReMarkInRoot` unless a later migration task explicitly requires them.
- Use one new change model: `change.Tracker`, `change.ChangeSet`, and generated `*Store` wrappers.
- ChangeSet 的热路径使用字段编号记录变更目标：运行时提供 `change.FieldID` 和 `change.FieldResolver`，`txgen` 生成字段编号常量和字段名解析器。`Path` 只保留为兼容入口和 map/slice 等动态尾路径，避免在常规字段变更中反复保存完整字符串路径。
- Redis and MongoDB drivers are external adapters; change detection remains owned by `tx/change`.
- Tests for `tx` stay under `/Users/a47/workspace/tx/tests`.
- Tests for scaffold generation stay under `/Users/a47/workspace/txgen/tests`.
- Run Go commands with `/Users/a47/sdk/go1.25.6/bin/go`.

---

## Current Baseline

The current code already proves the first stage:

- `tx/anno` parses built-in and caller-defined annotations.
- `tx/astscan` reads struct definitions, field aliases, methods, type-level annotations, and field-level struct tags.
- `tx/meta` normalizes storage/redis/mongodb annotations.
- `txgen/tests/data_scan_test.go` scans `/Users/a47/workspace/txgen/input/data` and verifies `GDRole` type/field annotations.

This means the next work starts from metadata already available from templates. The remaining work is generation and runtime behavior.

## Target File Structure

### Runtime: `/Users/a47/workspace/tx`

- Create `change/types.go`
  - Defines `Kind`, `Path`, `FieldChange`, `ChangeSet`, `Tracker`, and `Filter`.
- Create `change/tracker.go`
  - Implements generic dirty tracking primitives used by generated stores.
- Create `change/codec.go`
  - Converts `ChangeSet` to map/document forms used by adapters.
- Create `change/errors.go`
  - Defines validation/runtime errors for invalid paths or duplicate changes.
- Create `storage/changes.go`
  - Defines storage-facing interfaces that consume `change.ChangeSet`.
- Create `storage/redis/adapter.go`
  - Applies `ChangeSet` to Redis using redis metadata.
- Create `storage/mongo/adapter.go`
  - Applies `ChangeSet` to MongoDB update documents.
- Create `storage/fusion/adapter.go`
  - Coordinates Redis immediate writes with delayed Mongo persistence.
- Create `storage/fusion/queue.go`
  - Defines delayed save/delete queue abstraction.
- Modify `meta/types.go`
  - Ensure storage metadata exposes redis subKey, mongo indexes, idField, routeBy, shardBy, delaySaveTime, delayDelTime.
- Modify `storage/resolve.go`
  - Resolve `meta.TypeSpec` into runtime storage configuration for adapters.
- Test under `tests/change_*_test.go` and `tests/storage_*_test.go`.

### Generator: `/Users/a47/workspace/txgen`

- Create `gen/config.go`
  - Defines input/output paths and generation options.
- Create `gen/scan.go`
  - Scans `input/data` and returns ordered metadata.
- Create `gen/render_data.go`
  - Generates `output/data/GD*.go`.
- Create `gen/render_store.go`
  - Generates `output/data/*Store.go`.
- Create `gen/render_support.go`
  - Generates shared generated support files in `output/data`.
- Create `gen/write.go`
  - Writes formatted Go files deterministically.
- Modify `main.go`
  - Runs the generator with default config.
- Create or modify tests in `tests/`.

---

## Task 1: Define Runtime Change Model In `tx/change`

**Files:**
- Create: `/Users/a47/workspace/tx/change/types.go`
- Create: `/Users/a47/workspace/tx/change/tracker.go`
- Create: `/Users/a47/workspace/tx/change/errors.go`
- Test: `/Users/a47/workspace/tx/tests/change_tracker_test.go`

**Interfaces:**
- Produces:
  - `type Kind string`
  - `const KindSet Kind = "set"`
  - `const KindUnset Kind = "unset"`
  - `const KindReplace Kind = "replace"`
  - `type FieldID uint16`
  - `const FieldNone FieldID = 0`
  - `type FieldResolver interface { FieldName(FieldID) string }`
  - `type Path []string`
  - `func (p Path) String() string`
  - `type FieldChange struct { Field FieldID; Path Path; Kind Kind; Value any }`
  - `func (c FieldChange) FullPath(resolver FieldResolver) Path`
  - `type ChangeSet struct { changes []FieldChange }`
  - `func (s *ChangeSet) Add(change FieldChange) error`
  - `func (s *ChangeSet) RemoveField(field FieldID, tail Path)`
  - `func (s ChangeSet) Changes() []FieldChange`
  - `func (s ChangeSet) Empty() bool`
  - `type Tracker struct { changes ChangeSet; allDirty bool }`
  - `func (t *Tracker) Touch(path Path, value any) error`
  - `func (t *Tracker) TouchField(field FieldID, value any) error`
  - `func (t *Tracker) TouchFieldPath(field FieldID, tail Path, value any) error`
  - `func (t *Tracker) UnTouch(path Path)`
  - `func (t *Tracker) UnTouchField(field FieldID)`
  - `func (t *Tracker) UnTouchFieldPath(field FieldID, tail Path)`
  - `func (t *Tracker) Replace(path Path, value any) error`
  - `func (t *Tracker) ReplaceFieldPath(field FieldID, tail Path, value any) error`
  - `func (t *Tracker) UnsetFieldPath(field FieldID, tail Path) error`
  - `func (t *Tracker) AllTouch()`
  - `func (t *Tracker) AllUnTouch()`
  - `func (t *Tracker) Changes() ChangeSet`

**FieldID 约定：**
- `FieldNone` 表示未使用字段编号，兼容手写 `Path` 的调用方式。
- `txgen` 生成的字段编号从 `1` 开始；如果内部需要 bitset，可以在生成代码里自行把 `FieldID - 1` 映射为 bit 位。
- 普通字段变更只记录 `FieldID`，例如 `Field: fieldGDRoleName`；复合字段内的动态成员只在 `Path` 中记录尾路径，例如 `Field: fieldGDRoleItems, Path: Path{"1001", "quantity"}`。
- 落地前由 resolver 把 `FieldID` 解析为字段别名，再与尾路径拼成完整路径，例如 `items.1001.quantity`。
- Consumes: none.

- [ ] **Step 1: Write failing tests for path, touch, replace, and clear**

Add this file:

```go
package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

func TestTrackerRecordsFieldChanges(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"role", "name"}, "alice"); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	if err := tracker.Replace(change.Path{"role", "items"}, map[int32]int64{1: 2}); err != nil {
		t.Fatalf("Replace returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 2 {
		t.Fatalf("changes len = %d, want 2: %#v", len(changes), changes)
	}
	if changes[0].Kind != change.KindSet || changes[0].Path.String() != "role.name" || changes[0].Value != "alice" {
		t.Fatalf("first change = %#v", changes[0])
	}
	if changes[1].Kind != change.KindReplace || changes[1].Path.String() != "role.items" {
		t.Fatalf("second change = %#v", changes[1])
	}
}

func TestTrackerUnTouchRemovesFieldChange(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"role", "name"}, "alice"); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	tracker.UnTouch(change.Path{"role", "name"})

	if !tracker.Changes().Empty() {
		t.Fatalf("changes = %#v, want empty", tracker.Changes().Changes())
	}
}

func TestTrackerAllTouchAndAllUnTouch(t *testing.T) {
	var tracker change.Tracker

	tracker.AllTouch()
	if !tracker.AllDirty() {
		t.Fatalf("AllDirty = false, want true")
	}
	tracker.AllUnTouch()
	if tracker.AllDirty() || !tracker.Changes().Empty() {
		t.Fatalf("tracker was not cleared")
	}
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run 'TestTracker' -count=1
```

Expected: FAIL because `github.com/ngnl47/tx/change` does not exist.

- [ ] **Step 3: Implement the change package**

Create `change/errors.go`:

```go
package change

import "errors"

var ErrEmptyPath = errors.New("change: empty path")
```

Create `change/types.go`:

```go
package change

import "strings"

type Kind string

const (
	KindSet     Kind = "set"
	KindUnset   Kind = "unset"
	KindReplace Kind = "replace"
)

type Path []string

func (p Path) String() string {
	return strings.Join(p, ".")
}

func (p Path) Equal(other Path) bool {
	if len(p) != len(other) {
		return false
	}
	for i := range p {
		if p[i] != other[i] {
			return false
		}
	}
	return true
}

type FieldChange struct {
	Path  Path
	Kind  Kind
	Value any
}

type ChangeSet struct {
	changes []FieldChange
}

func (s *ChangeSet) Add(change FieldChange) error {
	if len(change.Path) == 0 {
		return ErrEmptyPath
	}
	copied := FieldChange{
		Path:  append(Path(nil), change.Path...),
		Kind:  change.Kind,
		Value: change.Value,
	}
	for i, existing := range s.changes {
		if existing.Path.Equal(copied.Path) {
			s.changes[i] = copied
			return nil
		}
	}
	s.changes = append(s.changes, copied)
	return nil
}

func (s *ChangeSet) Remove(path Path) {
	for i, change := range s.changes {
		if change.Path.Equal(path) {
			s.changes = append(s.changes[:i], s.changes[i+1:]...)
			return
		}
	}
}

func (s ChangeSet) Changes() []FieldChange {
	out := make([]FieldChange, len(s.changes))
	copy(out, s.changes)
	return out
}

func (s ChangeSet) Empty() bool {
	return len(s.changes) == 0
}
```

Create `change/tracker.go`:

```go
package change

type Tracker struct {
	changes  ChangeSet
	allDirty bool
}

func (t *Tracker) Touch(path Path, value any) error {
	return t.changes.Add(FieldChange{Path: path, Kind: KindSet, Value: value})
}

func (t *Tracker) UnTouch(path Path) {
	t.changes.Remove(path)
}

func (t *Tracker) Replace(path Path, value any) error {
	return t.changes.Add(FieldChange{Path: path, Kind: KindReplace, Value: value})
}

func (t *Tracker) AllTouch() {
	t.allDirty = true
}

func (t *Tracker) AllUnTouch() {
	t.allDirty = false
	t.changes = ChangeSet{}
}

func (t *Tracker) AllDirty() bool {
	return t.allDirty
}

func (t *Tracker) Changes() ChangeSet {
	return t.changes
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run 'TestTracker' -count=1
```

Expected: PASS.

- [ ] **Step 5: Run full `tx` tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./...
```

Expected: PASS.

---

## Task 2: Add Storage-Facing Change Interfaces

**Files:**
- Create: `/Users/a47/workspace/tx/storage/changes.go`
- Create: `/Users/a47/workspace/tx/tests/storage_changes_test.go`

**Interfaces:**
- Consumes:
  - `change.ChangeSet`
  - `meta.TypeSpec`
- Produces:
  - `type ApplyOptions struct { Upsert bool; TTL int64 }`
  - `type ChangeApplierWithOptions interface { ApplyChangesWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.ChangeSet, storage.ApplyOptions) error }`
  - `type ChangeDocument map[string]any`
  - `func ChangeSetToDocument(change.ChangeSet) ChangeDocument`

- [ ] **Step 1: Write failing tests for storage document conversion**

Create `tests/storage_changes_test.go`:

```go
package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/storage"
)

func TestChangeSetToDocumentUsesDotPathKeys(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{Path: change.Path{"items", "1", "quantity"}, Kind: change.KindSet, Value: int64(3)}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	doc := storage.ChangeSetToDocument(set)
	if doc["items.1.quantity"] != int64(3) {
		t.Fatalf("doc = %#v", doc)
	}
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestChangeSetToDocumentUsesDotPathKeys -count=1
```

Expected: FAIL because `storage.ChangeSetToDocument` does not exist.

- [ ] **Step 3: Implement storage change interfaces**

Create `storage/changes.go`:

```go
package storage

import (
	"context"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

type ApplyOptions struct {
	Upsert bool
	TTL    int64
}

type ChangeApplierWithOptions interface {
	ApplyChangesWithOptions(context.Context, meta.TypeSpec, RecordKey, change.ChangeSet, ApplyOptions) error
}

type ChangeDocument map[string]any

func ChangeSetToDocument(set change.ChangeSet) ChangeDocument {
	doc := ChangeDocument{}
	for _, fieldChange := range set.Changes() {
		doc[fieldChange.Path.String()] = fieldChange.Value
	}
	return doc
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestChangeSetToDocumentUsesDotPathKeys -count=1
```

Expected: PASS.

---

## Task 3: Generate Data Structs Into `txgen/output/data`

**Files:**
- Create: `/Users/a47/workspace/txgen/gen/config.go`
- Create: `/Users/a47/workspace/txgen/gen/scan.go`
- Create: `/Users/a47/workspace/txgen/gen/write.go`
- Create: `/Users/a47/workspace/txgen/gen/render_data.go`
- Modify: `/Users/a47/workspace/txgen/main.go`
- Create: `/Users/a47/workspace/txgen/tests/generate_data_test.go`

**Interfaces:**
- Consumes:
  - `astscan.ScanDir`
  - `meta.TypeSpec`
- Produces:
  - `type Config struct { InputDir string; OutputDir string; PackageName string }`
  - `func DefaultConfig(root string) Config`
  - `type DataType struct { Spec meta.TypeSpec; TypeDirectives []anno.Directive; FieldDirectives map[string][]anno.Directive }`
  - `func GenerateData(config Config) error`

- [ ] **Step 1: Write failing integration test**

Create `tests/generate_data_test.go`:

```go
package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ngnl47/txgen/gen"
)

func TestGenerateDataWritesGDRoleOutput(t *testing.T) {
	root := repoRoot(t)
	outputDir := filepath.Join(root, "output", "data")
	if err := os.RemoveAll(outputDir); err != nil {
		t.Fatalf("RemoveAll returned error: %v", err)
	}

	err := gen.GenerateData(gen.DefaultConfig(root))
	if err != nil {
		t.Fatalf("GenerateData returned error: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(outputDir, "role.go"))
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"package data",
		"type GDRole struct",
		"RoleId int64",
		"`comment:\"角色编号\" json:\"roleId,omitempty\" bson:\"roleId,omitempty\" msgpack:\"roleId,omitempty\"`",
		"func NewGDRole() *GDRole",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("generated role.go missing %q\n%s", want, text[:min(len(text), 2000)])
		}
	}
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestGenerateDataWritesGDRoleOutput -count=1
```

Expected: FAIL because `gen.GenerateData` does not exist.

- [ ] **Step 3: Implement config and scanner**

Create `gen/config.go`:

```go
package gen

import "path/filepath"

type Config struct {
	InputDir    string
	OutputDir   string
	PackageName string
}

func DefaultConfig(root string) Config {
	return Config{
		InputDir:    filepath.Join(root, "input", "data"),
		OutputDir:   filepath.Join(root, "output", "data"),
		PackageName: "data",
	}
}
```

Create `gen/scan.go`:

```go
package gen

import (
	"sort"

	"github.com/ngnl47/tx/anno"
	"github.com/ngnl47/tx/astscan"
	"github.com/ngnl47/tx/meta"
)

type DataType struct {
	Spec            meta.TypeSpec
	TypeDirectives  []anno.Directive
	FieldDirectives map[string][]anno.Directive
}

func scanData(config Config) ([]DataType, error) {
	set, err := astscan.ScanDir(config.InputDir, astscan.Options{
		Annotations: dataAnnotationDefinitions(),
	})
	if err != nil {
		return nil, err
	}
	specs := set.TypeSpecs()
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Type.Ref.Name < specs[j].Type.Ref.Name
	})
	out := make([]DataType, 0, len(specs))
	for _, spec := range specs {
		fullName := spec.Type.Ref.FullName
		out = append(out, DataType{
			Spec:            spec,
			TypeDirectives:  set.TypeAnnotations(fullName),
			FieldDirectives: set.FieldDirectives[fullName],
		})
	}
	return out, nil
}

type genDataTag struct{}
type genEnumTag struct{}
type customExportTag struct{}
type ignoreClientTag struct{}
type commentTag struct{}
type enumTag struct{}

func dataAnnotationDefinitions() anno.Definitions {
	defs := anno.BuiltinDefinitions()
	defs = append(defs,
		anno.TypeTag[genDataTag]("genData"),
		anno.TypeTag[genEnumTag]("genEnum"),
		anno.TypeTag[customExportTag]("customExport"),
		anno.FieldTag[customExportTag]("customExport"),
		anno.TypeTag[ignoreClientTag]("ignoreClient"),
		anno.FieldTag[ignoreClientTag]("ignoreClient"),
		anno.TypeTag[commentTag]("comment"),
		anno.FieldTag[commentTag]("comment"),
		anno.FieldTag[enumTag]("enum"),
	)
	return defs
}
```

- [ ] **Step 4: Implement deterministic writer**

Create `gen/write.go`:

```go
package gen

import (
	"go/format"
	"os"
	"path/filepath"
)

func writeGoFile(path string, source []byte) error {
	formatted, err := format.Source(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, formatted, 0o644)
}
```

- [ ] **Step 5: Implement minimal data renderer**

Create `gen/render_data.go`:

```go
package gen

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/ngnl47/tx/meta"
)

func GenerateData(config Config) error {
	specs, err := scanData(config)
	if err != nil {
		return err
	}
	for _, dataType := range specs {
		if dataType.Spec.Type.Ref.Name != "GDRole" {
			continue
		}
		if err := renderDataType(config, dataType); err != nil {
			return err
		}
	}
	return nil
}

func renderDataType(config Config, dataType DataType) error {
	spec := dataType.Spec
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "package %s\n\n", config.PackageName)
	fmt.Fprintf(&buf, "type %s struct {\n", spec.Type.Ref.Name)
	for _, field := range spec.Type.Fields {
		alias := field.Alias
		if alias == "" {
			alias = lowerCamel(field.Name)
		}
		comment := directiveValue(dataType.FieldDirectives[alias], "comment")
		tag := fmt.Sprintf("`comment:%q json:%q bson:%q msgpack:%q`", comment, alias+",omitempty", alias+",omitempty", alias+",omitempty")
		fmt.Fprintf(&buf, "\t%s %s %s\n", field.Name, field.Type, tag)
	}
	fmt.Fprintf(&buf, "}\n\n")
	fmt.Fprintf(&buf, "func New%s() *%s {\n\treturn new(%s)\n}\n", spec.Type.Ref.Name, spec.Type.Ref.Name, spec.Type.Ref.Name)

	filename := strings.TrimSuffix(strings.TrimPrefix(spec.Type.Ref.Name, "GD"), "")
	path := filepath.Join(config.OutputDir, strings.ToLower(filename)+".go")
	return writeGoFile(path, buf.Bytes())
}

func directiveValue(directives []anno.Directive, name string) string {
	for _, directive := range directives {
		if directive.Name == name {
			if value := directive.Params["value"]; value != "" {
				return value
			}
		}
	}
	return ""
}

func lowerCamel(name string) string {
	if name == "" {
		return ""
	}
	runes := []rune(name)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}
```

- [ ] **Step 6: Wire main**

Modify `main.go`:

```go
package main

import (
	"log"
	"os"

	"github.com/ngnl47/txgen/gen"
)

func main() {
	root, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	if err := gen.GenerateData(gen.DefaultConfig(root)); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 7: Run data generation test**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestGenerateDataWritesGDRoleOutput -count=1
```

Expected: PASS with `/Users/a47/workspace/txgen/output/data/role.go` generated.

---

## Task 4: Generate Minimal Store For Scalar Fields

**Files:**
- Create: `/Users/a47/workspace/txgen/gen/render_store.go`
- Modify: `/Users/a47/workspace/txgen/gen/render_data.go`
- Test: `/Users/a47/workspace/txgen/tests/generate_store_test.go`

**Interfaces:**
- Consumes:
  - `change.Tracker`
  - generated data type.
- Produces generated code:
  - `type GDRoleStore struct { Value *GDRole; tracker change.Tracker }`
  - `func NewGDRoleStore(value *GDRole) *GDRoleStore`
  - `func (s *GDRoleStore) GetRoleId() int64`
  - `func (s *GDRoleStore) SetRoleId(value int64) *GDRoleStore`
  - `func (s *GDRoleStore) TouchRoleId() *GDRoleStore`
  - `func (s *GDRoleStore) UnTouchRoleId() *GDRoleStore`
  - `func (s *GDRoleStore) Changes() change.ChangeSet`

- [ ] **Step 1: Write failing store generation test**

Create `tests/generate_store_test.go`:

```go
package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ngnl47/txgen/gen"
)

func TestGenerateDataWritesGDRoleStore(t *testing.T) {
	root := repoRoot(t)
	outputDir := filepath.Join(root, "output", "data")
	if err := os.RemoveAll(outputDir); err != nil {
		t.Fatalf("RemoveAll returned error: %v", err)
	}

	if err := gen.GenerateData(gen.DefaultConfig(root)); err != nil {
		t.Fatalf("GenerateData returned error: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(outputDir, "role_store.go"))
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"type GDRoleStore struct",
		"tracker change.Tracker",
		"func NewGDRoleStore(value *GDRole) *GDRoleStore",
		"func (s *GDRoleStore) SetRoleId(value int64) *GDRoleStore",
		"change.Path{\"roleId\"}",
		"func (s *GDRoleStore) Changes() change.ChangeSet",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("generated role_store.go missing %q\n%s", want, text)
		}
	}
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestGenerateDataWritesGDRoleStore -count=1
```

Expected: FAIL because `role_store.go` is not generated.

- [ ] **Step 3: Render minimal scalar store**

Create `gen/render_store.go`:

```go
package gen

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/ngnl47/tx/meta"
)

func renderStore(config Config, spec meta.TypeSpec) error {
	var buf bytes.Buffer
	name := spec.Type.Ref.Name
	storeName := name + "Store"

	fmt.Fprintf(&buf, "package %s\n\n", config.PackageName)
	fmt.Fprintf(&buf, "import \"github.com/ngnl47/tx/change\"\n\n")
	fmt.Fprintf(&buf, "type %s struct {\n\tValue *%s\n\ttracker change.Tracker\n}\n\n", storeName, name)
	fmt.Fprintf(&buf, "func New%s(value *%s) *%s {\n\treturn &%s{Value: value}\n}\n\n", storeName, name, storeName, storeName)
	for _, field := range spec.Type.Fields {
		if !isScalar(field.Type) {
			continue
		}
		alias := field.Alias
		if alias == "" {
			alias = lowerCamel(field.Name)
		}
		fmt.Fprintf(&buf, "func (s *%s) Get%s() %s {\n\treturn s.Value.%s\n}\n\n", storeName, field.Name, field.Type, field.Name)
		fmt.Fprintf(&buf, "func (s *%s) Set%s(value %s) *%s {\n\tif s.Value.%s == value {\n\t\treturn s\n\t}\n\ts.Value.%s = value\n\t_ = s.tracker.Touch(change.Path{%q}, value)\n\treturn s\n}\n\n", storeName, field.Name, field.Type, storeName, field.Name, field.Name, alias)
		fmt.Fprintf(&buf, "func (s *%s) Touch%s() *%s {\n\t_ = s.tracker.Touch(change.Path{%q}, s.Value.%s)\n\treturn s\n}\n\n", storeName, field.Name, storeName, alias, field.Name)
		fmt.Fprintf(&buf, "func (s *%s) UnTouch%s() *%s {\n\ts.tracker.UnTouch(change.Path{%q})\n\treturn s\n}\n\n", storeName, field.Name, storeName, alias)
	}
	fmt.Fprintf(&buf, "func (s *%s) Changes() change.ChangeSet {\n\treturn s.tracker.Changes()\n}\n", storeName)

	filename := strings.ToLower(strings.TrimPrefix(name, "GD")) + "_store.go"
	return writeGoFile(filepath.Join(config.OutputDir, filename), buf.Bytes())
}

func isScalar(typeName string) bool {
	switch typeName {
	case "bool", "string", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64":
		return true
	default:
		return false
	}
}
```

Modify `GenerateData` in `gen/render_data.go` so it calls `renderStore(config, spec)` after `renderDataType(config, spec)`.

- [ ] **Step 4: Run store generation test**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestGenerateDataWritesGDRoleStore -count=1
```

Expected: PASS.

---

## Task 5: Compile Generated Output

**Files:**
- Create: `/Users/a47/workspace/txgen/output/data/.gitkeep`
- Modify: `/Users/a47/workspace/txgen/tests/generate_data_test.go`
- Modify: `/Users/a47/workspace/txgen/tests/generate_store_test.go`

**Interfaces:**
- Consumes generated code in `output/data`.
- Produces compile verification for generated files.

- [ ] **Step 1: Add compile check helper in tests**

Create helper in `tests/generated_compile_test.go`:

```go
package tests

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func testGeneratedOutputCompiles(t *testing.T) {
	t.Helper()
	root := repoRoot(t)
	cmd := exec.Command("/Users/a47/sdk/go1.25.6/bin/go", "test", "./output/data")
	cmd.Dir = root
	cmd.Env = append(cmd.Environ(),
		"GOCACHE=/private/tmp/tx-go-build",
		"GOTMPDIR=/private/tmp",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated output did not compile: %v\n%s", err, out)
	}
	_ = filepath.Join
}
```

- [ ] **Step 2: Call compile helper after generation tests**

In `TestGenerateDataWritesGDRoleOutput` and `TestGenerateDataWritesGDRoleStore`, add:

```go
testGeneratedOutputCompiles(t)
```

after file content assertions.

- [ ] **Step 3: Run txgen tests**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./...
```

Expected: PASS and generated `output/data` compiles.

---

## Task 6: Extend Store Generation For Pointers And Struct Fields

**Files:**
- Modify: `/Users/a47/workspace/txgen/gen/render_store.go`
- Test: `/Users/a47/workspace/txgen/tests/generate_nested_store_test.go`

**Interfaces:**
- Consumes:
  - `change.Path`
  - generated `*Store`
- Produces:
  - `Ptr<Type>Store` for pointer fields.
  - nested store getters such as `GetRankInfo() *PtrGDRankInfoStore`.
  - nested changes with path prefix such as `rankInfo.level`.

- [ ] **Step 1: Write failing nested store test**

Create `tests/generate_nested_store_test.go`:

```go
package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ngnl47/txgen/gen"
)

func TestGenerateDataWritesNestedPointerStores(t *testing.T) {
	root := repoRoot(t)
	outputDir := filepath.Join(root, "output", "data")
	if err := os.RemoveAll(outputDir); err != nil {
		t.Fatalf("RemoveAll returned error: %v", err)
	}
	if err := gen.GenerateData(gen.DefaultConfig(root)); err != nil {
		t.Fatalf("GenerateData returned error: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(outputDir, "role_store.go"))
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"type PtrGDRankInfoStore struct",
		"func (s *GDRoleStore) GetRankInfo() *PtrGDRankInfoStore",
		"change.Path{\"rankInfo\"}",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("generated role_store.go missing %q", want)
		}
	}
}
```

- [ ] **Step 2: Implement pointer store renderer**

Add rendering helpers that detect field types beginning with `*` and generate pointer store wrappers. The generated pointer store must contain:

```go
type PtrGDRankInfoStore struct {
	Value *GDRankInfo
	tracker change.Tracker
	prefix change.Path
}
```

and a parent getter:

```go
func (s *GDRoleStore) GetRankInfo() *PtrGDRankInfoStore {
	return &PtrGDRankInfoStore{
		Value: s.Value.RankInfo,
		prefix: change.Path{"rankInfo"},
	}
}
```

- [ ] **Step 3: Run nested store test**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestGenerateDataWritesNestedPointerStores -count=1
```

Expected: PASS.

---

## Task 7: Implement Redis And Mongo Change Documents

**Files:**
- Create: `/Users/a47/workspace/tx/storage/redis/document.go`
- Create: `/Users/a47/workspace/tx/storage/mongo/document.go`
- Test: `/Users/a47/workspace/tx/tests/storage_redis_document_test.go`
- Test: `/Users/a47/workspace/tx/tests/storage_mongo_document_test.go`

**Interfaces:**
- Consumes:
  - `change.ChangeSet`
  - `meta.TypeSpec`
- Produces:
  - `redis.BuildHashChanges(spec meta.TypeSpec, set change.ChangeSet) map[string]map[string]any`
  - `mongo.BuildUpdateDocument(set change.ChangeSet) map[string]any`

- [ ] **Step 1: Write Redis document test**

Create `tests/storage_redis_document_test.go`:

```go
package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestRedisBuildHashChangesGroupsByTopLevelPath(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1", "quantity"}, Kind: change.KindSet, Value: int64(5)})

	hashes := txredis.BuildHashChanges(set)
	if hashes["items"]["1.quantity"] != int64(5) {
		t.Fatalf("hashes = %#v", hashes)
	}
}
```

- [ ] **Step 2: Write Mongo document test**

Create `tests/storage_mongo_document_test.go`:

```go
package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestMongoBuildUpdateDocumentUsesSet(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	update := txmongo.BuildUpdateDocument(set)
	setDoc := update["$set"].(map[string]any)
	if setDoc["name"] != "alice" {
		t.Fatalf("update = %#v", update)
	}
}
```

- [ ] **Step 3: Implement Redis document builder**

Create `storage/redis/document.go`:

```go
package redis

import (
	"strings"

	"github.com/ngnl47/tx/change"
)

func BuildHashChanges(set change.ChangeSet) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, fieldChange := range set.Changes() {
		if len(fieldChange.Path) == 0 {
			continue
		}
		hash := fieldChange.Path[0]
		field := strings.Join(fieldChange.Path[1:], ".")
		if field == "" {
			field = hash
		}
		if out[hash] == nil {
			out[hash] = map[string]any{}
		}
		out[hash][field] = fieldChange.Value
	}
	return out
}
```

- [ ] **Step 4: Implement Mongo update builder**

Create `storage/mongo/document.go`:

```go
package mongo

import "github.com/ngnl47/tx/change"

func BuildUpdateDocument(set change.ChangeSet) map[string]any {
	setDoc := map[string]any{}
	unsetDoc := map[string]any{}
	for _, fieldChange := range set.Changes() {
		switch fieldChange.Kind {
		case change.KindSet, change.KindReplace:
			setDoc[fieldChange.Path.String()] = fieldChange.Value
		case change.KindUnset:
			unsetDoc[fieldChange.Path.String()] = ""
		}
	}
	update := map[string]any{}
	if len(setDoc) > 0 {
		update["$set"] = setDoc
	}
	if len(unsetDoc) > 0 {
		update["$unset"] = unsetDoc
	}
	return update
}
```

- [ ] **Step 5: Run storage document tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run 'TestRedisBuildHashChanges|TestMongoBuildUpdateDocument' -count=1
```

Expected: PASS.

---

## Task 8: Add Fusion Delay Queue Strategy

**Files:**
- Create: `/Users/a47/workspace/tx/storage/fusion/queue.go`
- Create: `/Users/a47/workspace/tx/storage/fusion/strategy.go`
- Test: `/Users/a47/workspace/tx/tests/storage_fusion_strategy_test.go`

**Interfaces:**
- Consumes:
  - `meta.TypeSpec`
  - `change.ChangeSet`
- Produces:
  - `type DelayedOperationKind string`
  - `type DelayedOperation struct { Kind DelayedOperationKind; Type meta.TypeRef; ID any; Changes change.ChangeSet; RunAfter time.Duration }`
  - `type DelayQueue interface { Enqueue(context.Context, DelayedOperation) error }`
  - `type Strategy struct { Redis storage.ChangeApplierWithOptions; MongoQueue DelayQueue }`
  - `func (s Strategy) ApplyChangesWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, change.ChangeSet, storage.ApplyOptions) error`

- [ ] **Step 1: Write fusion strategy test**

Create `tests/storage_fusion_strategy_test.go`:

```go
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
)

type recordingQueue struct {
	ops []fusion.DelayedOperation
}

func (q *recordingQueue) Enqueue(_ context.Context, op fusion.DelayedOperation) error {
	q.ops = append(q.ops, op)
	return nil
}

func TestFusionStrategyEnqueuesMongoSave(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySaveTime: 600 * time.Second},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}
	if err := strategy.ApplyChangesWithOptions(context.Background(), spec, key, set, storage.ApplyOptions{}); err != nil {
		t.Fatalf("ApplyChangesWithOptions returned error: %v", err)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
	if queue.ops[0].RunAfter != 600*time.Second {
		t.Fatalf("RunAfter = %s", queue.ops[0].RunAfter)
	}
}
```

- [ ] **Step 2: Implement queue and strategy**

Create `storage/fusion/queue.go`:

```go
package fusion

import (
	"context"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

type DelayedOperationKind string

const (
	DelayedSave DelayedOperationKind = "save"
	DelayedDel  DelayedOperationKind = "delete"
)

type DelayedOperation struct {
	Kind     DelayedOperationKind
	Type     meta.TypeRef
	ID       any
	Changes  change.ChangeSet
	RunAfter time.Duration
}

type DelayQueue interface {
	Enqueue(context.Context, DelayedOperation) error
}
```

Create `storage/fusion/strategy.go`:

```go
package fusion

import (
	"context"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

type Strategy struct {
	Redis      storage.ChangeApplierWithOptions
	MongoQueue DelayQueue
}

func (s Strategy) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet, options storage.ApplyOptions) error {
	if s.Redis != nil {
		if err := s.Redis.ApplyChangesWithOptions(ctx, spec, key, set, options); err != nil {
			return err
		}
	}
	if s.MongoQueue == nil {
		return nil
	}
	return s.MongoQueue.Enqueue(ctx, DelayedOperation{
		Kind:     DelayedSave,
		Type:     spec.Type.Ref,
		ID:       key.ID,
		Changes:  set,
		RunAfter: spec.Storage.DelaySaveTime,
	})
}
```

- [ ] **Step 3: Run fusion tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run TestFusionStrategyEnqueuesMongoSave -count=1
```

Expected: PASS.

---

## Task 9: Add Metadata Validation For Persistence

**Files:**
- Modify: `/Users/a47/workspace/tx/meta/validate.go`
- Test: `/Users/a47/workspace/tx/tests/meta_storage_validate_test.go`

**Interfaces:**
- Consumes:
  - `meta.TypeSpec`
- Produces:
  - validation errors when `idField`, `routeBy`, `shardBy`, redis subKey, or mongo index references are invalid.

- [ ] **Step 1: Write validation tests**

Create `tests/meta_storage_validate_test.go`:

```go
package tests

import (
	"errors"
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestValidateRejectsMissingRedisIDField(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", Alias: "roleId"}},
		},
		Redis: &meta.RedisSpec{IDField: "missing"},
	}
	err := meta.Validate(spec, spec)
	if !errors.Is(err, meta.ErrUnknownField) {
		t.Fatalf("err = %v, want ErrUnknownField", err)
	}
}

func TestValidateAcceptsRedisIDFieldAlias(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{{Name: "RoleId", Alias: "roleId"}},
		},
		Redis: &meta.RedisSpec{IDField: "roleId"},
	}
	if err := meta.Validate(spec, spec); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}
```

- [ ] **Step 2: Run validation tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./tests -run 'TestValidate.*RedisIDField' -count=1
```

Expected: PASS if current validation already covers this; otherwise FAIL and implement the missing alias check.

- [ ] **Step 3: Implement missing validation only if tests fail**

Modify `meta/validate.go` so `Redis.IDField`, `Redis.RouteBy`, `Redis.ShardBy`, `Mongo.IDField`, `Mongo.RouteBy`, and `Mongo.ShardBy` are checked against field aliases.

- [ ] **Step 4: Run full tx tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./...
```

Expected: PASS.

---

## Task 10: Document The New Runtime/Generator Boundary

**Files:**
- Modify: `/Users/a47/workspace/tx/docs/legacy_generation_workflow.md`
- Create: `/Users/a47/workspace/txgen/README.md`

**Interfaces:**
- Produces human-readable boundary docs.

- [ ] **Step 1: Update legacy framework workflow doc**

Append this section to `docs/legacy_generation_workflow.md`:

```markdown
## Tx 重写边界

Tx 重写不复刻 legacy framework 历史上并存的 `Mark`、`MarkInRoot`、`StoreRoot` 多套变更接口。

新的边界是：

- `tx/change` 负责运行时变更记录和 dirty tracking。
- `tx/storage` 负责把 `change.ChangeSet` 落地到 Redis、MongoDB 或 fusion 策略。
- `txgen` 负责读取 `input` 模板并生成 `output` 代码。
- 生成后的业务代码只依赖 `github.com/ngnl47/tx`，不依赖 `github.com/ngnl47/txgen`。
```

- [ ] **Step 2: Create txgen README**

Create `/Users/a47/workspace/txgen/README.md`:

```markdown
# txgen

`txgen` is the generator and integration-test project for Tx scaffold code.

- `input`: scaffold templates used as generator input.
- `output`: generated Go code used by tests.
- `gen`: generator implementation.
- `tests`: integration tests for scanning and generation.

Generated business code imports `github.com/ngnl47/tx` at runtime. It does not import `github.com/ngnl47/txgen`.
```

- [ ] **Step 3: Run tests**

Run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./...
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./...
```

Expected: both commands PASS.

---

## Final Verification

After all tasks are complete, run:

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./...

cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./...
```

Expected:

- `tx` all tests pass.
- `txgen` all tests pass.
- `txgen/output/data` contains generated data and store files.
- Generated code imports `github.com/ngnl47/tx/change` and does not import `github.com/ngnl47/txgen`.

## Execution Notes

- Implement tasks in order.
- Keep each task independently testable.
- Do not add Redis/Mongo network integration tests in the first pass; use document builders and fake queues.
- Add real Redis/Mongo integration tests only after adapters are stable and test containers or local services are explicitly configured.
- Do not add compatibility wrappers for legacy framework deprecated APIs during these tasks.
