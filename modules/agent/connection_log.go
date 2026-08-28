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
	txlog.InfoContext(ctx, "agent connection opened", "transport", transportName, "remote", remote)
	err := server.Serve(ctx, transport)
	args := []any{
		"transport", transportName,
		"remote", remote,
		"durationMs", time.Since(startedAt).Milliseconds(),
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
