//go:build unix

package actortrial

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	txactor "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"google.golang.org/protobuf/proto"
)

// 分层夹具只测正常通信成本。原生层不提供实体、邮箱及有界 pending 契约；
// 协议层复用原型 Call/响应清理，仅在目标回调中直接执行同一回显逻辑。
func newComparisonLayer(config comparisonConfig) (*comparisonPeer, error) {
	p := &Transport{namespace: config.Namespace, instance: config.Namespace + "-" + config.Mode,
		inbox: nats.NewInbox() + ".", maxPending: 4096, pending: make(map[string]*pendingCall)}
	conn, err := nats.Connect(config.URL, nats.Timeout(time.Second), nats.ReconnectBufSize(-1),
		nats.ClosedHandler(func(*nats.Conn) { p.closeRequests() }))
	if err != nil {
		return nil, err
	}
	p.conn = conn
	var peer *comparisonPeer
	var receive nats.MsgHandler
	if config.Backend == "nats-native" {
		receive = func(msg *nats.Msg) {
			if err := msg.Respond(msg.Data); err != nil {
				panic(err)
			}
		}
		peer = &comparisonPeer{endpoint: p.instance, call: func(ctx context.Context, _ int, payload []byte) error {
			msg, err := conn.RequestWithContext(ctx, p.subject(config.Target), payload)
			if err == nil && !bytes.Equal(msg.Data, payload) {
				err = errors.New("原生响应不匹配")
			}
			return err
		}}
	} else {
		replyTrial := config.Backend == "nats-reply-unique" || config.Backend == "nats-reply-fixed"
		_, err = conn.Subscribe(p.inbox+"*", func(msg *nats.Msg) {
			key := msg.Subject
			if replyTrial {
				// 两组同样解码关联 ID，再复用原型清理路径；额外解码不能算作原型成本。
				var result actorpb.Result
				if err := proto.Unmarshal(msg.Data, &result); err == nil && result.RequestId != 0 {
					key = p.inbox + strconv.FormatUint(result.RequestId, 10)
				}
			}
			p.finishRequest(key, msg, nil)
		})
		receive = func(msg *nats.Msg) {
			var id Identity
			identity, err := base64.RawURLEncoding.DecodeString(msg.Header.Get("Actor-Identity"))
			if err == nil {
				err = json.Unmarshal(identity, &id)
			}
			if err != nil || id.Kind == "" || id.Key == "" {
				p.reply(msg, nil, errors.New("actortrial: invalid identity"))
				return
			}
			var delivery actorpb.Delivery
			if err = proto.Unmarshal(msg.Data, &delivery); err != nil {
				p.reply(msg, nil, err)
				return
			}
			request, err := txactor.DeliveryFromProto(&delivery)
			if err != nil {
				p.reply(msg, nil, err)
				return
			}
			result := comparisonReply(request)
			response := &actorpb.Result{MessageType: result.MessageType, CommandId: result.CommandID, Payload: append([]byte(nil), result.Payload...)}
			if replyTrial {
				// 仅正常回复改变主题；原请求仍有唯一 Reply，broker 的 503 可以精确关联。
				end := strings.LastIndexByte(msg.Reply, '.')
				response.RequestId, err = strconv.ParseUint(msg.Reply[end+1:], 10, 64)
				if err != nil {
					panic(err)
				}
				if config.Backend == "nats-reply-fixed" {
					msg.Reply = msg.Reply[:end+1] + "shared"
				}
			}
			p.reply(msg, response, resultError(response))
		}
		peer = comparisonTransportPeer(p, config.Target)
	}
	if err == nil {
		_, err = conn.Subscribe(p.subject(p.instance), receive)
	}
	if err == nil {
		err = conn.FlushTimeout(time.Second)
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	// 驱动先等全部调用结束再关闭；这里不冒充 Actor 的业务排水验收。
	peer.close = func() error { conn.Close(); return nil }
	return peer, nil
}
