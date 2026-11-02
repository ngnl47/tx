package actorrpc

import (
	"encoding/json"
	"log/slog"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/modules/discovery"
)

type routeInvalidator struct {
	invalidate func(discovery.SlotAssignment)
}

// BindRouteInvalidator 只接入条件失效；通知不携带业务正文，也不触发业务重投。
func (p *NATSTransport) BindRouteInvalidator(invalidate func(discovery.SlotAssignment)) {
	p.routeInvalidator.Store(&routeInvalidator{invalidate: invalidate})
}
func (p *NATSTransport) InvalidateRoute(observed discovery.SlotAssignment) {
	if observed.Generation == 0 {
		return
	}
	if handler := p.routeInvalidator.Load(); handler != nil {
		handler.invalidate(observed)
	}
}

// RejectRoute 复用现有 NATS 控制连接，仅在框架拒绝时通知来源实例。
func (p *NATSTransport) RejectRoute(source string, observed discovery.SlotAssignment, err error) {
	if err == nil || source == "" || !actorhost.IsRouteRejection(err) || observed.Validate() != nil {
		return
	}
	if source == p.instance {
		p.InvalidateRoute(observed)
		return
	}
	data, encodeErr := json.Marshal(observed)
	if encodeErr == nil {
		encodeErr = p.conn.PublishMsg(&nats.Msg{Subject: p.subject(source), Data: data, Header: nats.Header{"Actor-Route-Failure": []string{"1"}}})
	}
	if encodeErr != nil {
		slog.Warn("actor route invalidation notification failed", "err", encodeErr)
	}
}
func (p *NATSTransport) receiveRouteFailure(msg *nats.Msg) {
	var observed discovery.SlotAssignment
	if json.Unmarshal(msg.Data, &observed) == nil && observed.Validate() == nil {
		p.InvalidateRoute(observed)
	}
}
func observedRoute(msg *nats.Msg) discovery.SlotAssignment {
	generation, _ := strconv.ParseUint(msg.Header.Get("Actor-Generation"), 10, 64)
	return discovery.SlotAssignment{SlotID: msg.Header.Get("Actor-Slot"), RuntimeInstanceID: msg.Header.Get("Actor-Owner"), Generation: generation}
}
