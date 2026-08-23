// Package mongodriver 使用官方 MongoDB driver 实现 WAL 单分片客户端。
package mongodriver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/wal"
	walmongo "github.com/ngnl47/tx/wal/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const streamPositionIndex = "wal_stream_position_unique"

const readCursorBatchSize = 16

var errMongoDatabaseNotConfigured = errors.New("wal/mongo: database is not configured")

var (
	walJournal      = true
	walWriteConcern = &writeconcern.WriteConcern{W: "majority", Journal: &walJournal}
)

type document struct {
	Namespace      string    `bson:"namespace"`
	StreamKey      string    `bson:"streamKey"`
	Generation     int64     `bson:"generation"`
	Seq            int64     `bson:"seq"`
	PayloadVersion int64     `bson:"payloadVersion"`
	PayloadHash    []byte    `bson:"payloadHash"`
	Payload        []byte    `bson:"payload"`
	CreatedAt      time.Time `bson:"createdAt"`
}

// Database 将官方 MongoDB Database 适配为 WAL 的单分片客户端。
type Database struct {
	db *mongo.Database
}

var _ walmongo.Client = Database{}

// NewDatabase 创建 Mongo WAL 客户端。连接所有权仍归 server runtime。
func NewDatabase(db *mongo.Database) Database {
	return Database{db: db}
}

func (database Database) EnsureIndexes(ctx context.Context, collection string) error {
	if database.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := database.collection(collection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "namespace", Value: 1},
			{Key: "streamKey", Value: 1},
			{Key: "generation", Value: 1},
			{Key: "seq", Value: 1},
		},
		Options: options.Index().SetName(streamPositionIndex).SetUnique(true),
	})
	return mongoBackendError("ensure WAL indexes", err)
}

func (database Database) Insert(ctx context.Context, collection string, record wal.Record) error {
	if database.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := database.collection(collection).InsertOne(ctx, encodeRecord(record))
	if mongo.IsDuplicateKeyError(err) {
		return walmongo.ErrDuplicatePosition
	}
	return mongoBackendError("append WAL record", err)
}

func (database Database) Load(ctx context.Context, collection string, stream wal.Stream, seq uint64) (wal.Record, bool, error) {
	if database.db == nil {
		return wal.Record{}, false, errMongoDatabaseNotConfigured
	}
	var stored document
	err := database.collection(collection).FindOne(ctx, streamPositionFilter(stream, seq)).Decode(&stored)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return wal.Record{}, false, nil
	}
	if err != nil {
		return wal.Record{}, false, mongoBackendError("load WAL record", err)
	}
	record, err := decodeRecord(stream, stored)
	if err != nil {
		return wal.Record{}, false, err
	}
	return record, true, nil
}

func (database Database) Read(ctx context.Context, collection string, stream wal.Stream, afterSeq uint64, limit int) ([]wal.Record, error) {
	if database.db == nil {
		return nil, errMongoDatabaseNotConfigured
	}
	if limit <= 0 || limit > wal.MaxReadBatch {
		return nil, fmt.Errorf("%w: read limit must be between 1 and %d", wal.ErrInvalidRecord, wal.MaxReadBatch)
	}
	batchSize := limit
	if batchSize > readCursorBatchSize {
		batchSize = readCursorBatchSize
	}
	cursor, err := database.collection(collection).Find(ctx, bson.M{
		"namespace":  stream.Namespace,
		"streamKey":  stream.StreamKey,
		"generation": int64(stream.Generation),
		"seq":        bson.M{"$gt": int64(afterSeq)},
	}, options.Find().SetSort(bson.D{{Key: "seq", Value: 1}}).SetLimit(int64(limit)).SetBatchSize(int32(batchSize)))
	if err != nil {
		return nil, mongoBackendError("read WAL records", err)
	}
	defer cursor.Close(ctx)

	records := make([]wal.Record, 0, limit)
	payloadBytes := 0
	for cursor.Next(ctx) {
		var stored document
		if err := cursor.Decode(&stored); err != nil {
			return nil, fmt.Errorf("wal/mongo: decode record: %w", err)
		}
		record, err := decodeRecord(stream, stored)
		if err != nil {
			return nil, err
		}
		if len(record.Payload) > wal.MaxReadBatchBytes-payloadBytes {
			break
		}
		records = append(records, record)
		payloadBytes += len(record.Payload)
	}
	if err := cursor.Err(); err != nil {
		return nil, mongoBackendError("read WAL cursor", err)
	}
	return records, nil
}

func (database Database) Trim(ctx context.Context, collection string, stream wal.Stream, throughSeq uint64) error {
	if database.db == nil {
		return errMongoDatabaseNotConfigured
	}
	_, err := database.collection(collection).DeleteMany(ctx, bson.M{
		"namespace":  stream.Namespace,
		"streamKey":  stream.StreamKey,
		"generation": int64(stream.Generation),
		"seq":        bson.M{"$lte": int64(throughSeq)},
	})
	return mongoBackendError("trim WAL records", err)
}

func (database Database) collection(name string) *mongo.Collection {
	return database.db.Collection(name, options.Collection().SetWriteConcern(walWriteConcern))
}

func encodeRecord(record wal.Record) document {
	return document{
		Namespace:      record.Stream.Namespace,
		StreamKey:      record.Stream.StreamKey,
		Generation:     int64(record.Stream.Generation),
		Seq:            int64(record.Seq),
		PayloadVersion: int64(record.PayloadVersion),
		PayloadHash:    record.PayloadHash[:],
		Payload:        record.Payload,
		CreatedAt:      record.CreatedAt,
	}
}

func decodeRecord(stream wal.Stream, stored document) (wal.Record, error) {
	if len(stored.PayloadHash) != sha256.Size {
		return wal.Record{}, fmt.Errorf("%w: payload hash length is %d", wal.ErrInvalidRecord, len(stored.PayloadHash))
	}
	if stored.Generation <= 0 || stored.Seq <= 0 || stored.PayloadVersion <= 0 || stored.PayloadVersion > int64(^uint32(0)) {
		return wal.Record{}, fmt.Errorf("%w: invalid persisted numeric fields", wal.ErrInvalidRecord)
	}
	var hash [sha256.Size]byte
	copy(hash[:], stored.PayloadHash)
	record := wal.Record{
		Stream: wal.Stream{
			Namespace:  stored.Namespace,
			StreamKey:  stored.StreamKey,
			Generation: uint64(stored.Generation),
		},
		Seq:            uint64(stored.Seq),
		PayloadVersion: uint32(stored.PayloadVersion),
		PayloadHash:    hash,
		Payload:        stored.Payload,
		CreatedAt:      stored.CreatedAt.UTC(),
	}
	if err := record.Validate(); err != nil {
		return wal.Record{}, err
	}
	return record, nil
}

func streamPositionFilter(stream wal.Stream, seq uint64) bson.M {
	return bson.M{
		"namespace":  stream.Namespace,
		"streamKey":  stream.StreamKey,
		"generation": int64(stream.Generation),
		"seq":        int64(seq),
	}
}

func mongoBackendError(operation string, err error) error {
	if err == nil || (!mongo.IsNetworkError(err) && !mongo.IsTimeout(err)) {
		return err
	}
	return storage.NewBackendError(storage.BackendMongo, operation, err)
}
