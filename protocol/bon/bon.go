// Package bon 实现客户端协议载荷使用的二进制对象表示。
package bon

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	typeNull byte = iota
	typeInt
	typeLong
	typeFloat
	typeDouble
	typeString
	typeBool
	typeBinary
	typeDocument
	typeArray
	typeDateTime
	typeStringRef byte = 99
)

const maxContainerDepth = 64

// Marshal 使用 Go 值的 msgpack field tag 完成序列化。
func Marshal(value any) ([]byte, error) {
	writer := newWriter()
	if err := writer.writeReflect(reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	return writer.result(), nil
}

// Unmarshal 将完整载荷解码到 target；target 必须是非 nil 指针。
func Unmarshal(payload []byte, target any) error {
	reader := newReader(payload)
	value, err := reader.readValue()
	if err != nil {
		return err
	}
	if !reader.done() {
		return fmt.Errorf("bon: trailing payload data")
	}
	return DecodeValue(value, target)
}

// MarshalEnvelope 将协议命令序列化为 {c: command, b: body}。
func MarshalEnvelope(command string, body any) ([]byte, error) {
	if command == "" {
		return nil, fmt.Errorf("bon: command is empty")
	}
	writer := newWriter()
	writer.writeType(typeDocument)
	writer.writeNatural(2)
	writer.writeString("c")
	writer.writeString(command)
	writer.writeString("b")
	if err := writer.writeReflect(reflect.ValueOf(body)); err != nil {
		return nil, err
	}
	return writer.result(), nil
}

// UnmarshalEnvelope 解码完整的协议命令载荷。
func UnmarshalEnvelope(payload []byte) (command string, body any, err error) {
	reader := newReader(payload)
	value, err := reader.readValue()
	if err != nil {
		return "", nil, err
	}
	if !reader.done() {
		return "", nil, fmt.Errorf("bon: trailing payload data")
	}
	document, ok := value.(map[string]any)
	if !ok {
		return "", nil, fmt.Errorf("bon: command envelope must be a document")
	}
	command, ok = document["c"].(string)
	if !ok || command == "" {
		return "", nil, fmt.Errorf("bon: command envelope has invalid c")
	}
	body, ok = document["b"]
	if !ok {
		return "", nil, fmt.Errorf("bon: command envelope has no b")
	}
	return command, body, nil
}

// DecodeValue 使用 msgpack tag 将已经解码的 BON 值映射到 target。
func DecodeValue(value any, target any) error {
	if target == nil {
		return fmt.Errorf("bon: target is nil")
	}
	destination := reflect.ValueOf(target)
	if destination.Kind() != reflect.Pointer || destination.IsNil() {
		return fmt.Errorf("bon: target must be a non-nil pointer")
	}
	return assignValue(destination.Elem(), value)
}

type writer struct {
	bytes   []byte
	strings map[string]int
}

func newWriter() *writer {
	return &writer{bytes: make([]byte, 0, 256), strings: make(map[string]int)}
}

func (writer *writer) result() []byte {
	return append([]byte(nil), writer.bytes...)
}

func (writer *writer) writeReflect(value reflect.Value) error {
	if !value.IsValid() {
		writer.writeType(typeNull)
		return nil
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			writer.writeType(typeNull)
			return nil
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			writer.writeType(typeNull)
			return nil
		}
		return writer.writeReflect(value.Elem())
	}
	if value.Type() == timeType {
		writer.writeLong(value.Interface().(time.Time).UnixMilli(), typeDateTime)
		return nil
	}
	switch value.Kind() {
	case reflect.Bool:
		writer.writeType(typeBool)
		if value.Bool() {
			writer.writeByte(1)
		} else {
			writer.writeByte(0)
		}
	case reflect.Int8, reflect.Int16, reflect.Int32:
		writer.writeInt(int32(value.Int()))
	case reflect.Int, reflect.Int64:
		writer.writeLong(value.Int(), typeLong)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		writer.writeInt(int32(value.Uint()))
	case reflect.Uint, reflect.Uint64:
		if value.Uint() > math.MaxInt64 {
			return fmt.Errorf("bon: %s value %d exceeds int64", value.Type(), value.Uint())
		}
		writer.writeLong(int64(value.Uint()), typeLong)
	case reflect.Float32:
		writer.writeFloat(float32(value.Float()))
	case reflect.Float64:
		writer.writeDouble(value.Float())
	case reflect.String:
		writer.writeString(value.String())
	case reflect.Array, reflect.Slice:
		if value.Kind() == reflect.Slice && value.IsNil() {
			writer.writeType(typeNull)
			return nil
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			writer.writeType(typeBinary)
			writer.writeNatural(value.Len())
			for index := 0; index < value.Len(); index++ {
				writer.writeByte(byte(value.Index(index).Uint()))
			}
			return nil
		}
		writer.writeType(typeArray)
		writer.writeNatural(value.Len())
		for index := 0; index < value.Len(); index++ {
			if err := writer.writeReflect(value.Index(index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if value.IsNil() {
			writer.writeType(typeNull)
			return nil
		}
		writer.writeType(typeDocument)
		keys := value.MapKeys()
		writer.writeNatural(len(keys))
		for _, key := range keys {
			name, err := mapKeyString(key)
			if err != nil {
				return err
			}
			writer.writeString(name)
			if err := writer.writeReflect(value.MapIndex(key)); err != nil {
				return err
			}
		}
	case reflect.Struct:
		return writer.writeStruct(value)
	default:
		return fmt.Errorf("bon: unsupported type %s", value.Type())
	}
	return nil
}

func (writer *writer) writeStruct(value reflect.Value) error {
	meta := cachedStructMeta(value.Type())
	count := 0
	for _, field := range meta.fields {
		value, ok := readStructField(value, field.index)
		if !ok || (field.omitEmpty && value.IsZero()) {
			continue
		}
		count++
	}
	writer.writeType(typeDocument)
	writer.writeNatural(count)
	for _, field := range meta.fields {
		value, ok := readStructField(value, field.index)
		if !ok || (field.omitEmpty && value.IsZero()) {
			continue
		}
		writer.writeString(field.name)
		if err := writer.writeReflect(value); err != nil {
			return err
		}
	}
	return nil
}

type structFieldMeta struct {
	name      string
	index     []int
	omitEmpty bool
}

type structMeta struct {
	fields []structFieldMeta
	byName map[string]structFieldMeta
}

var structMetas sync.Map

func cachedStructMeta(typeOf reflect.Type) *structMeta {
	if cached, ok := structMetas.Load(typeOf); ok {
		return cached.(*structMeta)
	}
	meta := &structMeta{byName: make(map[string]structFieldMeta)}
	collectStructFields(typeOf, nil, meta)
	actual, _ := structMetas.LoadOrStore(typeOf, meta)
	return actual.(*structMeta)
}

func collectStructFields(typeOf reflect.Type, prefix []int, meta *structMeta) {
	for index := 0; index < typeOf.NumField(); index++ {
		definition := typeOf.Field(index)
		if definition.PkgPath != "" {
			continue
		}
		name, omitEmpty, ignored := msgpackField(definition)
		if ignored {
			continue
		}
		path := append(append([]int(nil), prefix...), index)
		if definition.Anonymous && name == "" {
			nested := definition.Type
			if nested.Kind() == reflect.Pointer {
				nested = nested.Elem()
			}
			if nested.Kind() == reflect.Struct {
				collectStructFields(nested, path, meta)
				continue
			}
		}
		if name == "" {
			name = lowerCamel(definition.Name)
		}
		field := structFieldMeta{name: name, index: path, omitEmpty: omitEmpty}
		meta.fields = append(meta.fields, field)
		if _, exists := meta.byName[name]; !exists {
			meta.byName[name] = field
		}
	}
}

func readStructField(value reflect.Value, index []int) (reflect.Value, bool) {
	for _, next := range index {
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return reflect.Value{}, false
			}
			value = value.Elem()
		}
		value = value.Field(next)
	}
	return value, true
}

func writeStructField(value reflect.Value, index []int) (reflect.Value, bool) {
	for _, next := range index {
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				if !value.CanSet() {
					return reflect.Value{}, false
				}
				value.Set(reflect.New(value.Type().Elem()))
			}
			value = value.Elem()
		}
		value = value.Field(next)
	}
	return value, value.CanSet()
}

func (writer *writer) writeType(value byte) { writer.writeByte(value) }
func (writer *writer) writeInt(value int32) {
	writer.writeType(typeInt)
	var buffer [4]byte
	binary.LittleEndian.PutUint32(buffer[:], uint32(value))
	writer.bytes = append(writer.bytes, buffer[:]...)
}
func (writer *writer) writeLong(value int64, kind byte) {
	writer.writeType(kind)
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:], uint64(value))
	writer.bytes = append(writer.bytes, buffer[:]...)
}
func (writer *writer) writeFloat(value float32) {
	writer.writeType(typeFloat)
	var buffer [4]byte
	binary.LittleEndian.PutUint32(buffer[:], math.Float32bits(value))
	writer.bytes = append(writer.bytes, buffer[:]...)
}
func (writer *writer) writeDouble(value float64) {
	writer.writeType(typeDouble)
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:], math.Float64bits(value))
	writer.bytes = append(writer.bytes, buffer[:]...)
}
func (writer *writer) writeString(value string) {
	if index, ok := writer.strings[value]; ok {
		writer.writeType(typeStringRef)
		writer.writeNatural(index)
		return
	}
	writer.writeType(typeString)
	writer.strings[value] = len(writer.strings)
	writer.writeNatural(len(value))
	writer.bytes = append(writer.bytes, value...)
}
func (writer *writer) writeNatural(value int) {
	next := uint64(value)
	for next >= 0x80 {
		writer.writeByte(byte(next) | 0x80)
		next >>= 7
	}
	writer.writeByte(byte(next))
}
func (writer *writer) writeByte(value byte) { writer.bytes = append(writer.bytes, value) }

type reader struct {
	bytes   []byte
	pos     int
	strings []string
}

func newReader(payload []byte) *reader {
	return &reader{bytes: payload, strings: make([]string, 0, 16)}
}
func (reader *reader) done() bool { return reader.pos == len(reader.bytes) }

func (reader *reader) readValue() (any, error) {
	return reader.readValueAtDepth(0)
}

func (reader *reader) readValueAtDepth(depth int) (any, error) {
	kind, err := reader.readByte()
	if err != nil {
		return nil, err
	}
	switch kind {
	case typeNull:
		return nil, nil
	case typeInt:
		return reader.readInt()
	case typeLong:
		return reader.readLong()
	case typeFloat:
		return reader.readFloat()
	case typeDouble:
		return reader.readDouble()
	case typeString:
		return reader.readString()
	case typeStringRef:
		return reader.readStringRef()
	case typeBool:
		value, err := reader.readByte()
		return value != 0, err
	case typeBinary:
		return reader.readBinary()
	case typeDocument:
		if depth >= maxContainerDepth {
			return nil, fmt.Errorf("bon: container depth exceeds %d", maxContainerDepth)
		}
		return reader.readDocument(depth + 1)
	case typeArray:
		if depth >= maxContainerDepth {
			return nil, fmt.Errorf("bon: container depth exceeds %d", maxContainerDepth)
		}
		return reader.readArray(depth + 1)
	case typeDateTime:
		value, err := reader.readLong()
		if err != nil {
			return nil, err
		}
		return time.UnixMilli(value), nil
	default:
		return nil, fmt.Errorf("bon: unknown value type %d", kind)
	}
}

func (reader *reader) readDocument(depth int) (map[string]any, error) {
	count, err := reader.readNatural()
	if err != nil {
		return nil, err
	}
	// 每个成员至少包含一个两字节的字符串键和一个一字节的值类型。
	if count > reader.remaining()/3 {
		return nil, fmt.Errorf("bon: document length %d exceeds remaining payload", count)
	}
	document := make(map[string]any, count)
	for index := 0; index < count; index++ {
		key, err := reader.readValueAtDepth(depth)
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("bon: document key is not string")
		}
		value, err := reader.readValueAtDepth(depth)
		if err != nil {
			return nil, err
		}
		document[name] = value
	}
	return document, nil
}

func (reader *reader) readArray(depth int) ([]any, error) {
	count, err := reader.readNatural()
	if err != nil {
		return nil, err
	}
	// 每个元素至少需要一个值类型字节，先拒绝不可能的数量再分配切片。
	if count > reader.remaining() {
		return nil, fmt.Errorf("bon: array length %d exceeds remaining payload", count)
	}
	values := make([]any, count)
	for index := range values {
		value, err := reader.readValueAtDepth(depth)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

func (reader *reader) readInt() (int32, error) {
	bytes, err := reader.readBytes(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(bytes)), nil
}
func (reader *reader) readLong() (int64, error) {
	bytes, err := reader.readBytes(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(bytes)), nil
}
func (reader *reader) readFloat() (float32, error) {
	bytes, err := reader.readBytes(4)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(bytes)), nil
}
func (reader *reader) readDouble() (float64, error) {
	bytes, err := reader.readBytes(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(bytes)), nil
}
func (reader *reader) readString() (string, error) {
	length, err := reader.readNatural()
	if err != nil {
		return "", err
	}
	bytes, err := reader.readBytes(length)
	if err != nil {
		return "", err
	}
	value := string(bytes)
	reader.strings = append(reader.strings, value)
	return value, nil
}
func (reader *reader) readStringRef() (string, error) {
	index, err := reader.readNatural()
	if err != nil {
		return "", err
	}
	if index >= len(reader.strings) {
		return "", fmt.Errorf("bon: string reference %d out of range", index)
	}
	return reader.strings[index], nil
}
func (reader *reader) readBinary() ([]byte, error) {
	length, err := reader.readNatural()
	if err != nil {
		return nil, err
	}
	bytes, err := reader.readBytes(length)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), bytes...), nil
}
func (reader *reader) readNatural() (int, error) {
	var value uint32
	for shift := uint(0); ; shift += 7 {
		if shift >= 35 {
			return 0, fmt.Errorf("bon: invalid 7-bit length")
		}
		next, err := reader.readByte()
		if err != nil {
			return 0, err
		}
		if shift == 28 && next&0xf0 != 0 {
			return 0, fmt.Errorf("bon: invalid 7-bit length")
		}
		value |= uint32(next&0x7f) << shift
		if next&0x80 == 0 {
			if uint64(value) > uint64(maxInt()) {
				return 0, fmt.Errorf("bon: length exceeds int range")
			}
			return int(value), nil
		}
	}
}
func (reader *reader) readByte() (byte, error) {
	if reader.pos >= len(reader.bytes) {
		return 0, fmt.Errorf("bon: unexpected end of payload")
	}
	value := reader.bytes[reader.pos]
	reader.pos++
	return value, nil
}
func (reader *reader) readBytes(length int) ([]byte, error) {
	if length < 0 || len(reader.bytes)-reader.pos < length {
		return nil, fmt.Errorf("bon: unexpected end of payload")
	}
	value := reader.bytes[reader.pos : reader.pos+length]
	reader.pos += length
	return value, nil
}

func (reader *reader) remaining() int {
	return len(reader.bytes) - reader.pos
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

var timeType = reflect.TypeFor[time.Time]()

func assignValue(destination reflect.Value, source any) error {
	if !destination.CanSet() {
		return fmt.Errorf("bon: destination %s cannot be set", destination.Type())
	}
	if source == nil {
		destination.SetZero()
		return nil
	}
	if destination.Kind() == reflect.Pointer {
		if destination.IsNil() {
			destination.Set(reflect.New(destination.Type().Elem()))
		}
		return assignValue(destination.Elem(), source)
	}
	if destination.Type() == timeType {
		value, ok := source.(time.Time)
		if !ok {
			return typeMismatch(destination.Type(), source)
		}
		destination.Set(reflect.ValueOf(value))
		return nil
	}
	switch destination.Kind() {
	case reflect.Interface:
		if destination.NumMethod() != 0 {
			return typeMismatch(destination.Type(), source)
		}
		destination.Set(reflect.ValueOf(source))
		return nil
	case reflect.Bool:
		value, ok := source.(bool)
		if !ok {
			return typeMismatch(destination.Type(), source)
		}
		destination.SetBool(value)
	case reflect.String:
		value, ok := source.(string)
		if !ok {
			return typeMismatch(destination.Type(), source)
		}
		destination.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value, err := integerValue(source)
		if err != nil {
			return err
		}
		if destination.OverflowInt(value) {
			return fmt.Errorf("bon: %d overflows %s", value, destination.Type())
		}
		destination.SetInt(value)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value, err := integerValue(source)
		if err != nil {
			return err
		}
		if value < 0 || destination.OverflowUint(uint64(value)) {
			return fmt.Errorf("bon: %d overflows %s", value, destination.Type())
		}
		destination.SetUint(uint64(value))
	case reflect.Float32, reflect.Float64:
		value, err := floatValue(source)
		if err != nil {
			return err
		}
		if destination.OverflowFloat(value) {
			return fmt.Errorf("bon: %v overflows %s", value, destination.Type())
		}
		destination.SetFloat(value)
	case reflect.Slice:
		if destination.Type().Elem().Kind() == reflect.Uint8 {
			value, ok := source.([]byte)
			if !ok {
				return typeMismatch(destination.Type(), source)
			}
			destination.SetBytes(value)
			return nil
		}
		values, ok := source.([]any)
		if !ok {
			return typeMismatch(destination.Type(), source)
		}
		output := reflect.MakeSlice(destination.Type(), len(values), len(values))
		for index, value := range values {
			if err := assignValue(output.Index(index), value); err != nil {
				return err
			}
		}
		destination.Set(output)
	case reflect.Array:
		values, ok := source.([]any)
		if !ok {
			return typeMismatch(destination.Type(), source)
		}
		if len(values) != destination.Len() {
			return fmt.Errorf("bon: array length %d does not match %s", len(values), destination.Type())
		}
		for index, value := range values {
			if err := assignValue(destination.Index(index), value); err != nil {
				return err
			}
		}
	case reflect.Map:
		document, ok := source.(map[string]any)
		if !ok {
			return typeMismatch(destination.Type(), source)
		}
		output := reflect.MakeMapWithSize(destination.Type(), len(document))
		for name, value := range document {
			key := reflect.New(destination.Type().Key()).Elem()
			if err := assignMapKey(key, name); err != nil {
				return err
			}
			entry := reflect.New(destination.Type().Elem()).Elem()
			if err := assignValue(entry, value); err != nil {
				return err
			}
			output.SetMapIndex(key, entry)
		}
		destination.Set(output)
	case reflect.Struct:
		document, ok := source.(map[string]any)
		if !ok {
			return typeMismatch(destination.Type(), source)
		}
		meta := cachedStructMeta(destination.Type())
		for name, value := range document {
			definition, ok := meta.byName[name]
			if !ok {
				continue
			}
			field, ok := writeStructField(destination, definition.index)
			if !ok {
				return fmt.Errorf("bon: destination field %q cannot be set", name)
			}
			if err := assignValue(field, value); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("bon: unsupported destination %s", destination.Type())
	}
	return nil
}

func msgpackField(field reflect.StructField) (name string, omitEmpty bool, ignored bool) {
	tag := field.Tag.Get("msgpack")
	if tag == "-" {
		return "", false, true
	}
	parts := strings.Split(tag, ",")
	if len(parts) > 0 {
		name = parts[0]
	}
	for _, option := range parts[1:] {
		if option == "omitempty" {
			omitEmpty = true
		}
	}
	return name, omitEmpty, false
}
func lowerCamel(value string) string {
	if value == "" {
		return ""
	}
	return strings.ToLower(value[:1]) + value[1:]
}
func mapKeyString(value reflect.Value) (string, error) {
	switch value.Kind() {
	case reflect.String:
		return value.String(), nil
	case reflect.Bool:
		return strconv.FormatBool(value.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(value.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(value.Uint(), 10), nil
	default:
		return "", fmt.Errorf("bon: unsupported map key %s", value.Type())
	}
}
func assignMapKey(destination reflect.Value, value string) error {
	switch destination.Kind() {
	case reflect.String:
		destination.SetString(value)
	case reflect.Bool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		destination.SetBool(parsed)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := strconv.ParseInt(value, 10, destination.Type().Bits())
		if err != nil {
			return err
		}
		destination.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsed, err := strconv.ParseUint(value, 10, destination.Type().Bits())
		if err != nil {
			return err
		}
		destination.SetUint(parsed)
	default:
		return fmt.Errorf("bon: unsupported map key %s", destination.Type())
	}
	return nil
}
func integerValue(value any) (int64, error) {
	switch typed := value.(type) {
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	default:
		return 0, typeMismatch(reflect.TypeFor[int64](), value)
	}
}
func floatValue(value any) (float64, error) {
	switch typed := value.(type) {
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	default:
		return 0, typeMismatch(reflect.TypeFor[float64](), value)
	}
}
func typeMismatch(want reflect.Type, got any) error {
	return fmt.Errorf("bon: cannot decode %T into %s", got, want)
}
