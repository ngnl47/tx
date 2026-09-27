package msgq

import "testing"

func BenchmarkBacklogSnapshot10K(b *testing.B) {
	server := &Server{sessions: make(map[int64]*mailboxSession, 10_000)}
	for sessionID := int64(1); sessionID <= 10_000; sessionID++ {
		session := newMailboxSession(SessionHeader{RoleID: sessionID, SessionID: sessionID, ConnectionID: 1})
		session.active = true
		session.downMsgs.enqueue([]byte("payload"))
		server.sessions[sessionID] = session
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		server.BacklogSnapshot()
	}
}
