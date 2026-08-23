package mongo_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/ngnl47/tx/wal"
	walmongo "github.com/ngnl47/tx/wal/mongo"
)

type routeCall struct {
	routeType string
	shardKey  any
}

type fakeRouter struct {
	endpoint string
	calls    []routeCall
}

func (router *fakeRouter) Route(_ context.Context, routeType string, shardKey any) (string, error) {
	router.calls = append(router.calls, routeCall{routeType: routeType, shardKey: shardKey})
	return router.endpoint, nil
}

type fakeClient struct {
	indexes    []string
	records    map[string]wal.Record
	trimmedSeq uint64
}

func (client *fakeClient) EnsureIndexes(_ context.Context, collection string) error {
	client.indexes = append(client.indexes, collection)
	return nil
}

func (client *fakeClient) Insert(_ context.Context, _ string, record wal.Record) error {
	if client.records == nil {
		client.records = map[string]wal.Record{}
	}
	key := recordKey(record.Stream, record.Seq)
	if _, ok := client.records[key]; ok {
		return walmongo.ErrDuplicatePosition
	}
	client.records[key] = record
	return nil
}

func (client *fakeClient) Load(_ context.Context, _ string, stream wal.Stream, seq uint64) (wal.Record, bool, error) {
	record, ok := client.records[recordKey(stream, seq)]
	return record, ok, nil
}

func (client *fakeClient) Read(_ context.Context, _ string, stream wal.Stream, afterSeq uint64, limit int) ([]wal.Record, error) {
	records := make([]wal.Record, 0, len(client.records))
	for _, record := range client.records {
		if record.Stream.Namespace == stream.Namespace && record.Stream.StreamKey == stream.StreamKey && record.Stream.Generation == stream.Generation && record.Seq > afterSeq {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
	if len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func (client *fakeClient) Trim(_ context.Context, _ string, _ wal.Stream, throughSeq uint64) error {
	client.trimmedSeq = throughSeq
	return nil
}

func recordKey(stream wal.Stream, seq uint64) string {
	return fmt.Sprintf("%s/%s/%d/%d", stream.Namespace, stream.StreamKey, stream.Generation, seq)
}

func mustRecord(t *testing.T, stream wal.Stream, seq uint64, payload string) wal.Record {
	t.Helper()
	record, err := wal.NewRecord(stream, seq, 1, []byte(payload))
	if err != nil {
		t.Fatalf("NewRecord returned error: %v", err)
	}
	return record
}

func newCluster(t *testing.T, router *fakeRouter, client *fakeClient) *walmongo.Cluster {
	t.Helper()
	cluster, err := walmongo.NewCluster(context.Background(), router, map[string]walmongo.Client{
		"mongo-1": client,
	}, walmongo.Options{RouteType: "wal", Collection: "tx_wal"})
	if err != nil {
		t.Fatalf("NewCluster returned error: %v", err)
	}
	return cluster
}

func TestClusterEnsuresIndexesAndRoutesSameStreamByStableIdentity(t *testing.T) {
	router := &fakeRouter{endpoint: "mongo-1"}
	client := &fakeClient{}
	cluster := newCluster(t, router, client)
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}

	if err := cluster.Append(context.Background(), mustRecord(t, stream, 1, "frame")); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	nextGeneration := stream
	nextGeneration.Generation++
	if err := cluster.Append(context.Background(), mustRecord(t, nextGeneration, 1, "next")); err != nil {
		t.Fatalf("Append next generation returned error: %v", err)
	}
	if len(client.indexes) != 1 || client.indexes[0] != "tx_wal" {
		t.Fatalf("indexes = %#v, want tx_wal", client.indexes)
	}
	if len(router.calls) != 2 || router.calls[0].routeType != "wal" || router.calls[0].shardKey != router.calls[1].shardKey {
		t.Fatalf("route calls = %#v", router.calls)
	}
	if _, ok := router.calls[0].shardKey.(int64); !ok {
		t.Fatalf("stable route key type = %T, want int64", router.calls[0].shardKey)
	}
}

func TestClusterAppendTreatsIdenticalRetryAsSuccess(t *testing.T) {
	router := &fakeRouter{endpoint: "mongo-1"}
	client := &fakeClient{}
	cluster := newCluster(t, router, client)
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	record := mustRecord(t, stream, 1, "frame")

	if err := cluster.Append(context.Background(), record); err != nil {
		t.Fatalf("first Append returned error: %v", err)
	}
	if err := cluster.Append(context.Background(), record); err != nil {
		t.Fatalf("retry Append returned error: %v", err)
	}
}

func TestClusterAppendRejectsDifferentPayloadAtSamePosition(t *testing.T) {
	router := &fakeRouter{endpoint: "mongo-1"}
	client := &fakeClient{}
	cluster := newCluster(t, router, client)
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}

	if err := cluster.Append(context.Background(), mustRecord(t, stream, 1, "first")); err != nil {
		t.Fatalf("first Append returned error: %v", err)
	}
	err := cluster.Append(context.Background(), mustRecord(t, stream, 1, "second"))
	if !errors.Is(err, wal.ErrConflict) {
		t.Fatalf("second Append error = %v, want ErrConflict", err)
	}
	if bytes.Equal(client.records[recordKey(stream, 1)].Payload, []byte("second")) {
		t.Fatal("conflicting retry replaced the durable record")
	}
}

func TestClusterReadRejectsSequenceGapAndTrimDelegates(t *testing.T) {
	router := &fakeRouter{endpoint: "mongo-1"}
	client := &fakeClient{records: map[string]wal.Record{}}
	cluster := newCluster(t, router, client)
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	client.records[recordKey(stream, 1)] = mustRecord(t, stream, 1, "first")
	client.records[recordKey(stream, 3)] = mustRecord(t, stream, 3, "third")

	if _, err := cluster.Read(context.Background(), stream, 0, 10); !errors.Is(err, wal.ErrSequenceGap) {
		t.Fatalf("Read error = %v, want ErrSequenceGap", err)
	}
	if err := cluster.Trim(context.Background(), stream, 3); err != nil {
		t.Fatalf("Trim returned error: %v", err)
	}
	if client.trimmedSeq != 3 {
		t.Fatalf("trimmed sequence = %d, want 3", client.trimmedSeq)
	}
}
