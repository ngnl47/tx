package tests

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/msgq"
)

// 使用示例配置验证条数与字节两种边界，不将单邮箱额度解释为实例内存上限。
func TestMsgQConfiguredBacklogLimits(t *testing.T) {
	for _, test := range []struct {
		name        string
		size, count int
	}{
		{"messages", 1024, 1024}, {"bytes", 16 * 1024, 512},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			server, err := msgq.NewServer(msgq.ServerOptions{InstanceID: "msgq-limits", Store: newMailboxSessionStore(), MaxBacklogMessages: 1024, MaxBacklogBytes: 8 * 1024 * 1024})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.Stop(context.Background()); err != nil {
					t.Error(err)
				}
			})
			slow := msgq.SessionHeader{RoleID: 1, SessionID: 11, ConnectionID: 1}
			healthy := msgq.SessionHeader{RoleID: 2, SessionID: 22, ConnectionID: 2}
			for _, h := range []msgq.SessionHeader{slow, healthy} {
				if err := server.Init(ctx, msgq.InitRequest{Header: h}); err != nil {
					t.Fatal(err)
				}
			}
			publish := func(seq int) {
				t.Helper()
				payload := make([]byte, test.size)
				binary.LittleEndian.PutUint64(payload, uint64(seq))
				result, err := server.PubSession(ctx, slow.SessionID, payload)
				if err != nil || result.Seq != int64(seq) {
					t.Fatalf("publish %d: %+v %v", seq, result, err)
				}
			}
			for i := 1; i <= test.count; i++ {
				publish(i)
			}
			sessions, messages, bytes := server.BacklogSnapshot()
			if sessions != 2 || messages != test.count || bytes != test.size*test.count {
				t.Fatalf("at limit: %d/%d/%d", sessions, messages, bytes)
			}
			// Fetch 不释放额度；只有确认已发送前缀才释放。
			batch, err := server.FetchSome(ctx, msgq.FetchSomeRequest{Header: slow, Max: 1})
			if err != nil || len(batch.Messages) != 1 {
				t.Fatalf("fetch: %+v %v", batch, err)
			}
			if err := server.Confirm(ctx, msgq.ConfirmRequest{Header: slow, Ack: 1}); err != nil {
				t.Fatal(err)
			}
			publish(test.count + 1)
			// 更换物理连接后，从最后确认的位置恢复，正文与序号都必须保留。
			slow.ConnectionID++
			if err := server.Init(ctx, msgq.InitRequest{Header: slow, Restore: true, Ack: 1}); err != nil {
				t.Fatal(err)
			}
			batch, err = server.FetchSome(ctx, msgq.FetchSomeRequest{Header: slow})
			if err != nil || len(batch.Messages) != test.count {
				t.Fatalf("restore: count=%d err=%v", len(batch.Messages), err)
			}
			for i, message := range batch.Messages {
				want := uint64(i + 2)
				if message.Seq != int64(want) || len(message.Payload) != test.size || binary.LittleEndian.Uint64(message.Payload) != want {
					t.Fatalf("restored message %d differs", want)
				}
			}
			_, messages, bytes = server.BacklogSnapshot()
			if messages != test.count || bytes != test.size*test.count {
				t.Fatalf("fetch released backlog: %d/%d", messages, bytes)
			}
			if _, err := server.PubSession(ctx, slow.SessionID, make([]byte, test.size)); !errors.Is(err, msgq.ErrDownMsgOverflow) {
				t.Fatalf("overflow: %v", err)
			}
			slow.ConnectionID++
			if err := server.Init(ctx, msgq.InitRequest{Header: slow, Restore: true}); !errors.Is(err, msgq.ErrSessionOffline) {
				t.Fatalf("evicted restore: %v", err)
			}
			if _, err := server.PubSession(ctx, healthy.SessionID, []byte("healthy")); err != nil {
				t.Fatal(err)
			}
			message, err := server.Fetch(ctx, msgq.FetchRequest{Header: healthy})
			if err != nil || string(message.Payload) != "healthy" {
				t.Fatalf("healthy: %+v %v", message, err)
			}
			if err := server.Confirm(ctx, msgq.ConfirmRequest{Header: healthy, Ack: message.Seq}); err != nil {
				t.Fatal(err)
			}
			sessions, messages, bytes = server.BacklogSnapshot()
			if sessions != 1 || messages != 0 || bytes != 0 {
				t.Fatalf("after eviction and ack: %d/%d/%d", sessions, messages, bytes)
			}
			slow.SessionID = 33
			if err := server.Init(ctx, msgq.InitRequest{Header: slow}); err != nil {
				t.Fatalf("fresh session: %v", err)
			}
		})
	}
}
