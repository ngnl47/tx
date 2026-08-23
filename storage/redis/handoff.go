package redis

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

const redisClusterSlotCount = 16384

// HandoffOptions 控制 Fusion 热写随文档原子登记的交接标记。
// 这是基础设施高级接口，普通业务保存不需要直接构造。
type HandoffOptions struct {
	Marker            string
	MaxPendingPerSlot int
	token             string
}

// HandoffReceipt 标识一次具体的 Redis 热写。
// revision 和 token 必须同时匹配，旧生命周期的确认才不会误删新交接记录。
type HandoffReceipt struct {
	Revision string
	Token    string
}

// HandoffEntry 是恢复器从同槽 Hash 读取的一条交接记录。
type HandoffEntry struct {
	Marker string
	Token  string
}

// HandoffWriter 是 Fusion 热写和交接确认所需的 Redis 高级边界。
// 普通业务保存通过 server Storage 使用，不直接依赖该接口。
type HandoffWriter interface {
	storage.ChangeApplier
	ApplyChangesWithHandoff(context.Context, meta.TypeSpec, storage.RecordKey, change.Source, change.FieldNameResolver, change.ValueResolver, storage.ApplyOptions, HandoffOptions) (HandoffReceipt, error)
	ConfirmHandoff(context.Context, meta.TypeSpec, storage.RecordKey, string, HandoffReceipt) (bool, error)
}

var (
	redisHandoffKeysOnce sync.Once
	redisHandoffKeys     [redisClusterSlotCount]string
)

func validateHandoffOptions(options HandoffOptions) error {
	if strings.TrimSpace(options.Marker) == "" {
		return ErrInvalidHandoff
	}
	if options.MaxPendingPerSlot <= 0 {
		return ErrInvalidHandoff
	}
	return nil
}

// ReadHandoffs 从固定物理 slot 对应的已知 Hash 中读取有限数量的交接记录。
// cursor 是物理 slot 游标；该方法不会执行 SCAN 或遍历 Redis keyspace。
func (a *Adapter) ReadHandoffs(ctx context.Context, cursor uint64, slotLimit int, entryLimit int) ([]HandoffEntry, uint64, error) {
	if slotLimit <= 0 || entryLimit <= 0 {
		return nil, cursor, ErrInvalidHandoff
	}
	if slotLimit > redisClusterSlotCount {
		slotLimit = redisClusterSlotCount
	}
	client, err := a.scriptClient()
	if err != nil {
		return nil, cursor, err
	}
	markers, err := readHandoffSlots(ctx, client, cursor, slotLimit, entryLimit)
	if err != nil {
		return nil, cursor, err
	}
	return markers, (cursor + uint64(slotLimit)) % redisClusterSlotCount, nil
}

func readHandoffSlots(ctx context.Context, client ScriptClient, cursor uint64, slotLimit int, entryLimit int) ([]HandoffEntry, error) {
	calls := make([]ScriptCall, slotLimit)
	for index := range calls {
		slot := uint16((cursor + uint64(index)) % redisClusterSlotCount)
		calls[index] = ScriptCall{Script: readHandoffsScript, Keys: []string{redisHandoffKey(slot)}, Args: []any{entryLimit}}
	}
	var bodies []string
	var err error
	if batchClient, ok := client.(BatchScriptClient); ok && !isNilBatchScriptClient(batchClient) {
		bodies, err = batchClient.EvalBatch(ctx, calls)
	} else {
		bodies = make([]string, len(calls))
		for index, call := range calls {
			bodies[index], err = client.Eval(ctx, call.Script, call.Keys, call.Args...)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("redis: read fusion handoff slots from %d: %w", cursor, err)
	}
	if len(bodies) != len(calls) {
		return nil, fmt.Errorf("redis: read fusion handoff slots returned %d results for %d calls", len(bodies), len(calls))
	}
	entries := make([]HandoffEntry, 0)
	for index, body := range bodies {
		slot := uint16((cursor + uint64(index)) % redisClusterSlotCount)
		batch, err := decodeHandoffSlot(slot, body)
		if err != nil {
			return nil, err
		}
		entries = append(entries, batch...)
	}
	return entries, nil
}

func decodeHandoffSlot(slot uint16, body string) ([]HandoffEntry, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(body))
	var values []string
	if err := decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("redis: decode fusion handoffs slot %d: %w", slot, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("redis: decode fusion handoffs slot %d: multiple JSON values", slot)
		}
		return nil, fmt.Errorf("redis: decode fusion handoffs slot %d: invalid trailing JSON: %w", slot, err)
	}
	if len(values)%2 != 0 {
		return nil, fmt.Errorf("redis: decode fusion handoffs slot %d: odd marker/token count %d", slot, len(values))
	}
	entries := make([]HandoffEntry, 0, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		if strings.TrimSpace(values[index]) == "" || strings.TrimSpace(values[index+1]) == "" {
			return nil, fmt.Errorf("redis: decode fusion handoffs slot %d: empty marker or token", slot)
		}
		entries = append(entries, HandoffEntry{Marker: values[index], Token: values[index+1]})
	}
	return entries, nil
}

// EncodeHandoffMarker 使用与延迟任务一致的类型和记录键生成稳定交接标记。
// 这是 Fusion 与 Redis 删除边界共享的高级协议，不面向普通业务调用。
func EncodeHandoffMarker(ref meta.TypeRef, key storage.RecordKey) (string, error) {
	typeName := handoffTypeName(ref, key.TypeName)
	if typeName == "" || key.ID == nil {
		return "", ErrInvalidHandoff
	}
	body, err := json.Marshal(struct {
		Type     string `json:"type"`
		ID       any    `json:"id"`
		ShardKey any    `json:"shardKey,omitempty"`
	}{Type: typeName, ID: key.ID, ShardKey: key.ShardKey})
	if err != nil {
		return "", fmt.Errorf("redis: encode fusion handoff marker: %w", err)
	}
	return string(body), nil
}

func handoffTypeName(ref meta.TypeRef, fallback string) string {
	switch {
	case strings.TrimSpace(ref.FullName) != "":
		return strings.TrimSpace(ref.FullName)
	case strings.TrimSpace(ref.ImportPath) != "" && strings.TrimSpace(ref.Name) != "":
		return strings.TrimSpace(ref.ImportPath) + "." + strings.TrimSpace(ref.Name)
	case strings.TrimSpace(ref.Package) != "" && strings.TrimSpace(ref.Name) != "":
		return strings.TrimSpace(ref.Package) + "." + strings.TrimSpace(ref.Name)
	case strings.TrimSpace(ref.Name) != "":
		return strings.TrimSpace(ref.Name)
	default:
		return strings.TrimSpace(fallback)
	}
}

func newHandoffToken() (string, error) {
	var body [16]byte
	if _, err := rand.Read(body[:]); err != nil {
		return "", fmt.Errorf("redis: generate fusion handoff token: %w", err)
	}
	return hex.EncodeToString(body[:]), nil
}

func isNilBatchScriptClient(client BatchScriptClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func redisHandoffKey(slot uint16) string {
	redisHandoffKeysOnce.Do(buildRedisHandoffKeys)
	return redisHandoffKeys[slot]
}

func buildRedisHandoffKeys() {
	remaining := redisClusterSlotCount
	for candidate := uint64(0); remaining > 0; candidate++ {
		tag := strconv.FormatUint(candidate, 10)
		slot := redisClusterSlot(tag)
		if redisHandoffKeys[slot] != "" {
			continue
		}
		redisHandoffKeys[slot] = "tx:fusion:handoff:{" + tag + "}"
		remaining--
	}
}

func redisClusterSlot(key string) uint16 {
	tag := key
	start := strings.IndexByte(key, '{')
	if start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			tag = key[start+1 : start+1+end]
		}
	}
	return redisCRC16(tag) % redisClusterSlotCount
}

func redisCRC16(value string) uint16 {
	var crc uint16
	for i := 0; i < len(value); i++ {
		crc ^= uint16(value[i]) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
