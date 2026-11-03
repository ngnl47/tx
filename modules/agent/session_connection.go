package agent

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/ngnl47/tx/modules/msgq"
)

// activeSession 拥有一条物理连接的接入、收发和收尾资源，不拥有 MsgQ 逻辑邮箱。
type activeSession struct {
	identity  Identity
	header    msgq.SessionHeader
	client    msgq.SessionClient
	ctx       context.Context
	cancel    context.CancelFunc
	transport Transport

	// 初始化登记状态由 sessionRegistry.mu 保护，通过 initializationDone 公布结果。
	initializationDone chan struct{}
	initialized        bool
	suppressReady      bool
	terminateRequested bool

	// 初始化补偿协程写入终止结果，等待者在 terminationDone 关闭后读取。
	terminationDone chan struct{}
	terminationErr  error

	// 仅 serve 编排资源收尾；writerErr 在 writerDone 关闭后读取。
	mailbox     msgq.MailboxStreamSession
	stopMailbox func() bool
	writer      sessionFrameWriter
	writerDone  chan struct{}
	writerErr   error

	// 收发协程共享已写下行游标；closed 封住已读出但尚未准入的上行。
	writtenDownSeq atomic.Int64
	closed         atomic.Bool
}

func (session *activeSession) run(ctx context.Context, server *Server, handshake Frame) error {
	// 邮箱失败必须能中断正在写 Socket 的协程，不能等下一次 Recv 才感知。
	session.stopMailbox = context.AfterFunc(session.mailbox.Context(), session.disconnect)
	if handshake.Restore {
		session.writtenDownSeq.Store(handshake.Ack)
	}
	if err := session.writer.Write(session.ctx, Frame{Kind: FrameReady}); err != nil {
		return err
	}
	session.writerDone = make(chan struct{})
	go func() {
		session.writerErr = server.writeDownMsgs(session.ctx, session.mailbox, &session.writer, &session.writtenDownSeq)
		session.cancel()
		close(session.writerDone)
	}()
	return session.readFrames(ctx, server, handshake.Ack)
}

func (session *activeSession) readFrames(ctx context.Context, server *Server, confirmedDownSeq int64) error {
	// 上行游标仅属于当前物理连接，重连只续接 MsgQ 的下行邮箱。
	var acceptedUpSeq int64
	for {
		frame, err := session.transport.Read(session.ctx)
		if err != nil {
			return err
		}
		if frame.validLogout() && len(frame.Payload) == 0 {
			return session.logout(ctx)
		}
		if frame.validAck() {
			if err := session.writer.validateAcknowledgement(frame.Ack, confirmedDownSeq, &session.writtenDownSeq); err != nil {
				return err
			}
			if err := session.mailbox.Ack(session.ctx, frame.Ack); err != nil {
				return err
			}
			confirmedDownSeq = frame.Ack
			continue
		}
		if !frame.validUpMsg() {
			return ErrInvalidFrame
		}
		if err := session.writer.validateAcknowledgement(frame.Ack, confirmedDownSeq, &session.writtenDownSeq); err != nil {
			return err
		}
		// 这里是连接的逐帧准入点；此前已接纳的业务不因顶号撤回。
		if session.closed.Load() {
			return context.Canceled
		}
		if err := server.dispatchUpMsg(session.ctx, session.mailbox, session.identity, frame, &acceptedUpSeq, &confirmedDownSeq); err != nil {
			return err
		}
	}
}

func (session *activeSession) logout(ctx context.Context) error {
	session.closed.Store(true)
	// 先结束邮箱并等在途写完成，再回写确认；不提前取消 WebSocket 写入。
	session.stopMailbox()
	// 使用连接入口的上下文，下行协程退出对 session.ctx 的取消不能截断退出确认。
	logoutCtx, cancel := context.WithTimeout(ctx, sessionTerminationTimeout)
	defer cancel()
	if err := session.mailbox.Terminate(logoutCtx); err != nil {
		return err
	}
	select {
	case <-session.writerDone:
	case <-logoutCtx.Done():
		return logoutCtx.Err()
	}
	return session.writer.Write(logoutCtx, Frame{Kind: FrameLoggedOut})
}

func (session *activeSession) close(registry *sessionRegistry, serveErr error) error {
	session.cancel()
	if session.writerDone != nil {
		<-session.writerDone
		if cause := context.Cause(session.mailbox.Context()); cause != nil && !errors.Is(cause, context.Canceled) {
			serveErr = errors.Join(serveErr, cause)
		}
		if serveErr == nil && session.writerErr != nil && session.writerErr != context.Canceled {
			serveErr = session.writerErr
		}
	}
	if session.stopMailbox != nil {
		session.stopMailbox()
	}
	// 仅移除当前 ConnectionID，不影响已经替换进来的连接；未接入也可安全收尾。
	registry.unbind(session.header.SessionID, session.header.ConnectionID)
	if session.mailbox != nil {
		// 本地接入关闭后等待远端解除绑定；取消不替代确认，也不销毁逻辑邮箱。
		session.disconnect()
		serveErr = errors.Join(serveErr, session.mailbox.Close())
	}
	// 未取得邮箱时，Socket 留给 serve 发送接入拒绝通知后再关闭。
	return serveErr
}

func (session *activeSession) disconnect() {
	if session == nil {
		return
	}
	session.closed.Store(true)
	session.cancel()
	_ = session.transport.Close()
}

func (session *activeSession) completeTermination(err error) {
	session.terminationErr = err
	close(session.terminationDone)
}
