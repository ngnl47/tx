package etcd

import (
	"context"
	"errors"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrClientNil   = errors.New("etcd: client is nil")
	ErrWatchClosed = errors.New("etcd: watch closed")
)

// KeyValue 是与 etcd 响应缓冲区隔离的键值快照。
type KeyValue struct {
	Key            string
	Value          []byte
	CreateRevision int64
	ModRevision    int64
	LeaseID        int64
}

// Snapshot 是某个前缀在同一 etcd 修订版本下的键值集合。
type Snapshot struct {
	Revision int64
	Values   []KeyValue
}

// EventType 描述 Watch 事件类型。
type EventType uint8

const (
	EventPut EventType = iota + 1
	EventDelete
)

// Event 是一个与 etcd 响应缓冲区隔离的 Watch 键值变更。
type Event struct {
	Type  EventType
	Value KeyValue
}

// WatchResponse 承载一批 Watch 事件或终止 Watch 的错误。
type WatchResponse struct {
	Events []Event
	Err    error
}

// Client 提供 tx 基础设施使用的 etcd 键值原语。
type Client struct {
	raw       *clientv3.Client
	closeRaw  bool
	closeOnce sync.Once
	closeErr  error
}

// New 创建并持有一个底层 etcd 客户端。
func New(ctx context.Context, config Config) (*Client, error) {
	config, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	raw, err := clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		return nil, err
	}
	client, err := NewWithClient(raw, true)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return client, nil
}

// NewWithClient 包装已有客户端；closeClient 决定 Close 是否关闭它。
func NewWithClient(raw *clientv3.Client, closeClient bool) (*Client, error) {
	if raw == nil {
		return nil, ErrClientNil
	}
	return &Client{raw: raw, closeRaw: closeClient}, nil
}

// Close 释放 Client 持有的底层客户端。
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		if c.closeRaw {
			c.closeErr = c.raw.Close()
		}
	})
	return c.closeErr
}

// Probe 通过只读限量查询确认客户端能够访问 etcd 多数派。
func (c *Client) Probe(ctx context.Context) error {
	_, err := c.raw.Get(ctx, "/", clientv3.WithFromKey(), clientv3.WithLimit(1))
	return err
}

// Get 读取一个键及其修订版本。
func (c *Client) Get(ctx context.Context, key string) (KeyValue, bool, error) {
	response, err := c.raw.Get(ctx, key)
	if err != nil {
		return KeyValue{}, false, err
	}
	if len(response.Kvs) == 0 {
		return KeyValue{}, false, nil
	}
	return copyKeyValue(response.Kvs[0]), true, nil
}

// Snapshot 在同一修订版本读取指定前缀下的全部键值。
func (c *Client) Snapshot(ctx context.Context, prefix string) (Snapshot, error) {
	response, err := c.raw.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return Snapshot{}, err
	}
	values := make([]KeyValue, len(response.Kvs))
	for index, value := range response.Kvs {
		values[index] = copyKeyValue(value)
	}
	return Snapshot{Revision: response.Header.Revision, Values: values}, nil
}

// WatchPrefix 从指定修订版本开始监听前缀；非正值表示从当前版本开始。
func (c *Client) WatchPrefix(ctx context.Context, prefix string, revision int64) <-chan WatchResponse {
	options := []clientv3.OpOption{clientv3.WithPrefix()}
	if revision > 0 {
		options = append(options, clientv3.WithRev(revision))
	}
	rawResponses := c.raw.Watch(ctx, prefix, options...)
	responses := make(chan WatchResponse)
	go func() {
		defer close(responses)
		for {
			select {
			case <-ctx.Done():
				return
			case response, ok := <-rawResponses:
				if !ok {
					sendWatchError(ctx, responses, ErrWatchClosed)
					return
				}
				if err := response.Err(); err != nil {
					sendWatchError(ctx, responses, err)
					return
				}
				events := make([]Event, len(response.Events))
				for index, event := range response.Events {
					events[index] = copyEvent(event)
				}
				select {
				case responses <- WatchResponse{Events: events}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return responses
}

// Put 写入一个键，并可选择把它绑定到已有租约。
func (c *Client) Put(ctx context.Context, key string, value []byte, leaseID int64) error {
	_, err := c.raw.Put(ctx, key, string(value), putOptions(leaseID)...)
	return err
}

// Delete 删除一个键。
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.raw.Delete(ctx, key)
	return err
}

// Create 仅在键不存在时创建它，并返回创建后的修订信息。
func (c *Client) Create(ctx context.Context, key string, value []byte, leaseID int64) (KeyValue, bool, error) {
	response, err := c.raw.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value), putOptions(leaseID)...)).
		Commit()
	if err != nil {
		return KeyValue{}, false, err
	}
	if !response.Succeeded {
		return KeyValue{}, false, nil
	}
	revision := response.Header.Revision
	return KeyValue{
		Key:            key,
		Value:          append([]byte(nil), value...),
		CreateRevision: revision,
		ModRevision:    revision,
		LeaseID:        leaseID,
	}, true, nil
}

// CompareAndSwap 仅在键仍处于 expectedRevision 时替换它的值。
func (c *Client) CompareAndSwap(ctx context.Context, key string, expectedRevision int64, value []byte) (bool, error) {
	response, err := c.raw.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision)).
		Then(clientv3.OpPut(key, string(value), clientv3.WithIgnoreLease())).
		Commit()
	if err != nil {
		return false, err
	}
	return response.Succeeded, nil
}

func putOptions(leaseID int64) []clientv3.OpOption {
	if leaseID == 0 {
		return nil
	}
	return []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(leaseID))}
}

func copyKeyValue(value *mvccpb.KeyValue) KeyValue {
	return KeyValue{
		Key:            string(value.Key),
		Value:          append([]byte(nil), value.Value...),
		CreateRevision: value.CreateRevision,
		ModRevision:    value.ModRevision,
		LeaseID:        value.Lease,
	}
}

func copyEvent(event *clientv3.Event) Event {
	typeValue := EventPut
	if event.Type == clientv3.EventTypeDelete {
		typeValue = EventDelete
	}
	return Event{Type: typeValue, Value: copyKeyValue(event.Kv)}
}

func sendWatchError(ctx context.Context, responses chan<- WatchResponse, err error) {
	select {
	case responses <- WatchResponse{Err: err}:
	case <-ctx.Done():
	}
}

// CompareAndSwapWithinSnapshot 写入无租约键，同时确认前缀内现存键没有快照之后的写入。
// 删除不使检查失败；调用方仅可将它用于删除不会导致超额的计数准入。
// expectedRevision 为零表示创建，非零表示更新已有键。
func (c *Client) CompareAndSwapWithinSnapshot(ctx context.Context, prefix string, revision int64, key string, expectedRevision int64, value []byte) (bool, error) {
	response, err := c.raw.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(prefix), "<", revision+1).WithPrefix(),
		clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision),
	).Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return false, err
	}
	return response.Succeeded, nil
}
