package msgq

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 固定 1ms 关闭通知延迟，观察批量关闭是否把每个会话的往返串行累计。
func BenchmarkMailboxStopConfirmedSessions(b *testing.B) {
	for _, count := range []int{1, 64} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &admissionTestStore{}, MaxBacklogMessages: 4})
				if err != nil {
					b.Fatal(err)
				}
				client, _ := NewLocalSessionClient(mailbox)
				for id := int64(1); id <= int64(count); id++ {
					request := takeoverRequest(id, 1)
					request.Header.RoleID = id
					if _, err = client.OpenMailboxStream(context.Background(), request, func() { time.Sleep(time.Millisecond) }); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if err = mailbox.Stop(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
