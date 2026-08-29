package agent

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/coder/websocket"
	txlog "github.com/ngnl47/tx/log"
)

func serveConnection(ctx context.Context, transportName, remote string, server *Server, transport Transport) {
	startedAt := time.Now()
	connectionID, err := server.nextConnectionID()
	if err != nil {
		_ = transport.Close()
		txlog.WarnContext(ctx, "agent connection closed unexpectedly",
			"transport", transportName,
			"remote", remote,
			"durationMs", time.Since(startedAt).Milliseconds(),
			"err", err,
		)
		return
	}
	txlog.InfoContext(ctx, "agent connection opened", "transport", transportName, "remote", remote, "connectionID", connectionID)
	trace := sessionTrace{}
	err = server.serve(ctx, transport, connectionID, &trace)
	args := []any{
		"transport", transportName,
		"remote", remote,
		"connectionID", connectionID,
		"durationMs", time.Since(startedAt).Milliseconds(),
	}
	if trace.roleID > 0 && trace.sessionID > 0 {
		args = append(args, "roleID", trace.roleID, "sessionID", trace.sessionID)
	}
	if normalConnectionClose(ctx, err) {
		txlog.InfoContext(ctx, "agent connection closed", args...)
		return
	}
	args = append(args, "err", err)
	txlog.WarnContext(ctx, "agent connection closed unexpectedly", args...)
}

func normalConnectionClose(ctx context.Context, err error) bool {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway || status == websocket.StatusNoStatusRcvd
}
