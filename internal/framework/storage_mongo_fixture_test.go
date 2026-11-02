package framework

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

// 只模拟建连与健康探测协议，存储生命周期仍由真实驱动和内建 CMod 管理。
func newStorageMongoFixture(t *testing.T) string {
	t.Helper()
	address := newStorageTCPFixture(t, serveStorageMongoFixture)
	return "mongodb://" + address + "/rollback?directConnection=true&serverSelectionTimeoutMS=1000"
}

func newStorageTCPFixture(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var workers sync.WaitGroup
	connections := make(map[net.Conn]struct{})
	stopped := false
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if stopped {
				mu.Unlock()
				_ = conn.Close()
				return
			}
			connections[conn] = struct{}{}
			workers.Add(1)
			mu.Unlock()
			go func() {
				defer workers.Done()
				defer conn.Close()
				defer func() {
					mu.Lock()
					delete(connections, conn)
					mu.Unlock()
				}()
				serve(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		stopped = true
		for conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return listener.Addr().String()
}

func serveStorageMongoFixture(conn net.Conn) {
	for {
		var header [16]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		length, requestID, _, opcode, _, ok := wiremessage.ReadHeader(header[:])
		if !ok || length < 16 {
			return
		}
		body := make([]byte, length-16)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		command, err := storageMongoFixtureCommand(opcode, body)
		if err != nil {
			return
		}
		response := bson.D{{Key: "ok", Value: 1}}
		switch strings.ToLower(command) {
		case "hello", "ismaster":
			response = append(response,
				bson.E{Key: "ismaster", Value: true},
				bson.E{Key: "isWritablePrimary", Value: true},
				bson.E{Key: "helloOk", Value: true},
				bson.E{Key: "minWireVersion", Value: 0},
				bson.E{Key: "maxWireVersion", Value: 17},
				bson.E{Key: "maxBsonObjectSize", Value: 16 * 1024 * 1024},
				bson.E{Key: "maxMessageSizeBytes", Value: 48 * 1024 * 1024},
				bson.E{Key: "maxWriteBatchSize", Value: 100000},
			)
		case "ping", "endsessions":
		default:
			response = bson.D{{Key: "ok", Value: 0}, {Key: "errmsg", Value: "unsupported fixture command: " + command}, {Key: "code", Value: 59}}
		}
		document, err := bson.Marshal(response)
		if err != nil {
			return
		}
		var reply []byte
		if opcode == wiremessage.OpQuery {
			_, reply = wiremessage.AppendHeaderStart(nil, wiremessage.NextRequestID(), requestID, wiremessage.OpReply)
			reply = wiremessage.AppendReplyFlags(reply, 0)
			reply = wiremessage.AppendReplyCursorID(reply, 0)
			reply = wiremessage.AppendReplyStartingFrom(reply, 0)
			reply = wiremessage.AppendReplyNumberReturned(reply, 1)
		} else {
			_, reply = wiremessage.AppendHeaderStart(nil, wiremessage.NextRequestID(), requestID, wiremessage.OpMsg)
			reply = wiremessage.AppendMsgFlags(reply, 0)
			reply = wiremessage.AppendMsgSectionType(reply, wiremessage.SingleDocument)
		}
		reply = append(reply, document...)
		binary.LittleEndian.PutUint32(reply[:4], uint32(len(reply)))
		if _, err := conn.Write(reply); err != nil {
			return
		}
	}
}

func storageMongoFixtureCommand(opcode wiremessage.OpCode, body []byte) (string, error) {
	var document bsoncore.Document
	var ok bool
	switch opcode {
	case wiremessage.OpQuery:
		_, body, _ = wiremessage.ReadQueryFlags(body)
		_, body, _ = wiremessage.ReadQueryFullCollectionName(body)
		_, body, _ = wiremessage.ReadQueryNumberToSkip(body)
		_, body, _ = wiremessage.ReadQueryNumberToReturn(body)
		document, _, ok = wiremessage.ReadQueryQuery(body)
	case wiremessage.OpMsg:
		_, body, _ = wiremessage.ReadMsgFlags(body)
		_, body, _ = wiremessage.ReadMsgSectionType(body)
		document, _, ok = wiremessage.ReadMsgSectionSingleDocument(body)
	}
	if !ok {
		return "", fmt.Errorf("invalid Mongo fixture message: %s", opcode)
	}
	elements, err := document.Elements()
	if err != nil || len(elements) == 0 {
		return "", fmt.Errorf("invalid Mongo fixture command: %v", err)
	}
	return elements[0].Key(), nil
}
