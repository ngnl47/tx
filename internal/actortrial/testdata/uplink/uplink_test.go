//go:build unix

package actortrial

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	legacyagent "github.com/ngnl47/tx/internal/actortrial/testdata/agent"
	txactor "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	"github.com/ngnl47/tx/lifecycle"
	publicactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/agent"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/protocol"
	"github.com/ngnl47/tx/protocol/bon"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/redis/goredis"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"txgame-server/gen/data"
)

// 仅由 build.py 映入测试包；普通 go test 和生产构建均不依赖示例生成包。
// 固定拓扑隔离传输差异，成员变化与归属权威校验由 I1 真实集成测试覆盖。
type uplinkConfig struct {
	comparisonConfig
	Redis, MsgQ, World string
	Sessions           int
}

type uplinkAssignment discovery.SlotAssignment

func (s uplinkAssignment) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	return discovery.SlotAssignment(s), nil
}

// 客户端成功必须同时收到精确上行 ACK 和带请求关联号的下行；不以入队数代替完成数。
func validateUplinkReply(frame agent.Frame, seq int64, payload []byte) error {
	if frame.Kind == agent.FrameAck && frame.Ack == seq {
		return nil
	}
	if frame.Kind == agent.FrameDownMsg && frame.Seq == seq && bytes.Equal(frame.Payload, payload) {
		return nil
	}
	return errors.New("上行完成或下行响应不匹配")
}

func TestUplinkCompletionCheck(t *testing.T) {
	for _, frame := range []agent.Frame{
		{Kind: agent.FrameAck, Ack: 6},
		{Kind: agent.FrameDownMsg, Seq: 7, Payload: []byte("旧响应")},
		{Kind: agent.FrameDownMsg, Seq: 6, Payload: []byte("当前响应")},
	} {
		if validateUplinkReply(frame, 7, []byte("当前响应")) == nil {
			t.Fatal("旧确认被计为完成", frame)
		}
	}
}

type uplinkProcedure struct {
	store *data.ResRankItemStore
	fail  bool
}

func (p uplinkProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{txn.NewLockTarget(txn.NewLockKey("uplink", p.store.GetId()), p.store)}, nil
}
func (p uplinkProcedure) Process(*txn.Tx) error {
	p.store.SetResType(p.store.GetResType() + 1)
	for i := int64(1); i <= 4; i++ {
		item, _ := p.store.RankSnapDataStore().Get(i)
		item.SetVal(item.GetVal() + 1)
	}
	if p.fail {
		return errors.New("预期回滚")
	}
	return nil
}
func uplinkStore(id string) *data.ResRankItemStore {
	value := &data.ResRankItem{Id: id, RankSnapData: make(map[int64]*data.ResourceRank, 16)}
	for i := int64(1); i <= 16; i++ {
		value.RankSnapData[i] = &data.ResourceRank{RoleId: i}
	}
	return value.Store()
}
func TestUplinkGeneratedTransaction(t *testing.T) {
	store := uplinkStore("check")
	runtime := txn.NewRuntime()
	if err := runtime.Call(t.Context(), uplinkProcedure{store: store}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Call(t.Context(), uplinkProcedure{store: store, fail: true}); err == nil {
		t.Fatal("未回滚")
	}
	if store.GetResType() != 1 {
		t.Fatal("根字段回滚失败")
	}
	for i := int64(1); i <= 4; i++ {
		item, _ := store.RankSnapDataStore().Get(i)
		if item.GetVal() != 1 {
			t.Fatal("子字段回滚失败")
		}
	}
	if !store.HasChanges() {
		t.Fatal("已提交标脏丢失")
	}
}

type uplinkActor struct {
	receive func(context.Context, txactor.Delivery) (txactor.Result, error)
}

func (a uplinkActor) Receive(c protoactor.Context) {
	message, ok := c.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	d, err := txactor.DeliveryFromProto(message)
	var result txactor.Result
	if err == nil {
		result, err = a.receive(context.Background(), d)
	}
	txactor.Respond(c, d, result, err)
}

type uplinkBody struct {
	Seq     int64  `msgpack:"seq"`
	Padding []byte `msgpack:"padding"`
}

type uplinkSession struct {
	mu   sync.Mutex
	conn net.Conn
	seq  int64
}

func writeUplinkFrame(conn net.Conn, f agent.Frame) error {
	encoded, err := agent.EncodeFrame(f, 1<<20)
	if err != nil {
		return err
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(encoded)))
	buffers := net.Buffers{prefix[:], encoded}
	_, err = buffers.WriteTo(conn)
	return err
}
func readUplinkFrame(conn net.Conn) (agent.Frame, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		return agent.Frame{}, err
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size > 1<<20 {
		return agent.Frame{}, agent.ErrFrameTooLarge
	}
	encoded := make([]byte, size)
	if _, err := io.ReadFull(conn, encoded); err != nil {
		return agent.Frame{}, err
	}
	return agent.DecodeFrame(encoded, 1<<20)
}
func newUplinkClient(config uplinkConfig) (*comparisonPeer, error) {
	sessions := make([]uplinkSession, config.Sessions)
	closeAll := func() error {
		for i := range sessions {
			if sessions[i].conn != nil {
				_ = sessions[i].conn.Close()
			}
		}
		return nil
	}
	for i := range sessions {
		conn, err := net.DialTimeout("tcp", config.Target, 5*time.Second)
		if err != nil {
			_ = closeAll()
			return nil, err
		}
		sessions[i].conn = conn
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err = writeUplinkFrame(conn, agent.Frame{Kind: agent.FrameHandshake, Payload: []byte(strconv.Itoa(i + 1))}); err == nil {
			var f agent.Frame
			f, err = readUplinkFrame(conn)
			if err == nil && f.Kind != agent.FrameReady {
				err = agent.ErrHandshakeRequired
			}
		}
		if err != nil {
			_ = closeAll()
			return nil, err
		}
	}
	return &comparisonPeer{endpoint: "tcp-client", close: closeAll, call: func(ctx context.Context, id int, payload []byte) error {
		session := &sessions[id]
		session.mu.Lock()
		defer session.mu.Unlock()
		deadline, _ := ctx.Deadline()
		if err := session.conn.SetDeadline(deadline); err != nil {
			return err
		}
		seq := session.seq + 1
		body, err := bon.MarshalEnvelope("rank.update", uplinkBody{seq, payload})
		if err != nil {
			return err
		}
		expected, err := bon.MarshalEnvelope("rank.updated", uplinkBody{seq, payload})
		if err != nil {
			return err
		}
		err = writeUplinkFrame(session.conn, agent.Frame{Kind: agent.FrameUpMsg, Seq: seq, Ack: session.seq, CommandID: 47, RouteKey: protocol.Int64RouteKey(int64(id/2 + 1)), Payload: body})
		if err != nil {
			return err
		}
		ack, down := false, false
		for !ack || !down {
			f, err := readUplinkFrame(session.conn)
			if err != nil {
				return err
			}
			if err = validateUplinkReply(f, seq, expected); err != nil {
				return err
			}
			if f.Kind == agent.FrameAck {
				ack = true
			} else {
				down = true
			}
		}
		session.seq = seq
		return nil
	}}, nil
}

func newUplinkPeer(base comparisonConfig) (peer *comparisonPeer, err error) {
	var config uplinkConfig
	if err = json.Unmarshal([]byte(os.Getenv("TX_RPC_COMPARISON")), &config); err != nil {
		return nil, err
	}
	config.comparisonConfig = base
	if config.Mode == "client" {
		return newUplinkClient(config)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	var closers []func() error
	// 关闭顺序由测试组合根保存；依赖最后关闭。采样时只做显式 gc/snapshot。
	closeAll := func() error {
		var all error
		for i := len(closers) - 1; i >= 0; i-- {
			all = errors.Join(all, closers[i]())
		}
		root.Cancel()
		root.Wait()
		return all
	}
	defer func() {
		if err != nil {
			_ = closeAll()
		}
	}()
	peer = &comparisonPeer{close: closeAll}
	if config.Mode == "msgq" {
		client := redis.NewClient(&redis.Options{Addr: config.Redis})
		closers = append(closers, client.Close)
		if err = client.Ping(root).Err(); err != nil {
			return nil, err
		}
		store, e := msgq.NewRedisSessionStore(goredis.NewClient(client), msgq.SessionRedisOptions{Prefix: config.Namespace, TTL: time.Minute})
		if e != nil {
			return nil, e
		}
		mailbox, e := msgq.NewServer(msgq.ServerOptions{InstanceID: config.Namespace + "-msgq", Store: store, MaxBacklogMessages: 64})
		if e != nil {
			return nil, e
		}
		host, e := rpc.NewHost("127.0.0.1:0")
		if e != nil {
			return nil, e
		}
		closers = append(closers, func() error { root.Cancel(); return host.Stop(context.Background()) })
		if e = host.Register(func(r grpc.ServiceRegistrar) error { return msgq.RegisterMailboxService(r, mailbox) }); e != nil {
			return nil, e
		}
		if e = host.Start(root); e != nil {
			return nil, e
		}
		if e = mailbox.Start(root); e != nil {
			return nil, e
		}
		closers = append(closers, func() error { return mailbox.Stop(context.Background()) })
		peer.endpoint = host.Endpoint()
		return peer, nil
	}
	var queue msgq.SessionClient
	if config.Mode == "agent" || config.Mode == "game" {
		dialer, e := msgq.NewGRPCDialer(insecure.NewCredentials(), grpc.WithDisableRetry())
		if e != nil {
			return nil, e
		}
		queue, e = dialer.DialSession(root, discovery.Instance{Endpoint: config.MsgQ})
		if e != nil {
			return nil, e
		}
		closers = append(closers, queue.Close)
	}
	var manager *writeback.Manager
	executor := txn.NewRuntime()
	var adapter *txredis.Adapter
	var stores sync.Map
	if config.Mode == "game" {
		client := redis.NewClient(&redis.Options{Addr: config.Redis})
		closers = append(closers, client.Close)
		adapter, err = txredis.NewAdapter(goredis.NewClient(client))
		if err != nil {
			return nil, err
		}
		manager = writeback.NewManager(writeback.Dependencies{Writer: adapter}, writeback.Config{FlushInterval: 100 * time.Millisecond, ShutdownFlushTimeout: 10 * time.Second, MaxPendingRoots: 2048, HeapSampleInterval: time.Second})
		if err = executor.BindCommitParticipant(manager); err != nil {
			return nil, err
		}
		if err = manager.Start(root); err != nil {
			return nil, err
		}
		peer.verify = func(expected int) error {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if e := manager.Flush(ctx); e != nil {
				return e
			}
			total := int64(0)
			var verify error
			stores.Range(func(_, v any) bool {
				store := v.(*data.ResRankItemStore)
				loaded, exists, e := data.LoadResRankItemStore(ctx, adapter, store.RecordKey())
				if e != nil || !exists {
					verify = fmt.Errorf("最终回读: exists=%v err=%v", exists, e)
					return false
				}
				if loaded.GetResType() != store.GetResType() {
					verify = errors.New("最终事务计数不匹配")
					return false
				}
				total += loaded.GetResType()
				for i := int64(1); i <= 4; i++ {
					item, _ := loaded.RankSnapDataStore().Get(i)
					if item.GetVal() != loaded.GetResType() {
						verify = errors.New("最终子字段不匹配")
						return false
					}
				}
				return true
			})
			if verify != nil {
				return verify
			}
			if total != int64(expected) {
				return fmt.Errorf("完成 %d 次，持久化事务 %d 次", expected, total)
			}
			return nil
		}
		closers = append(closers, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			return manager.Shutdown(ctx)
		})
	}
	var cross func(context.Context, txactor.Delivery) (txactor.Result, error)
	factory := func() func(context.Context, txactor.Delivery) (txactor.Result, error) {
		var store *data.ResRankItemStore
		return func(ctx context.Context, d txactor.Delivery) (txactor.Result, error) {
			if config.Mode == "world" {
				return txactor.Result{MessageType: "world.done", Payload: d.Payload}, nil
			}
			if d.Identity == nil || d.Identity.RoleID <= 0 || d.UpSeq <= 0 {
				return txactor.Result{}, errors.New("无有效会话")
			}
			command, raw, e := bon.UnmarshalEnvelope(d.Payload)
			if e != nil || command != "rank.update" {
				return txactor.Result{}, fmt.Errorf("业务解码: %s %v", command, e)
			}
			var body uplinkBody
			if e = bon.DecodeValue(raw, &body); e != nil {
				return txactor.Result{}, e
			}
			if body.Seq != d.UpSeq {
				return txactor.Result{}, errors.New("业务关联号不匹配")
			}
			if store == nil {
				store = uplinkStore(config.Namespace + "-" + strconv.FormatInt(d.Identity.RoleID, 10))
				if e := storage.Create(ctx, adapter, store); e != nil {
					return txactor.Result{}, e
				}
				stores.Store(d.Identity.RoleID, store)
			}
			if err := executor.Call(ctx, uplinkProcedure{store: store}); err != nil {
				return txactor.Result{}, err
			}
			// 错开 Session 相位，让预热和各短窗口都接近 10% 跨 Actor 调用。
			if (d.Identity.SessionID+d.UpSeq)%10 == 0 {
				callCtx, cancel := context.WithTimeout(ctx, time.Second)
				response, e := cross(callCtx, txactor.Delivery{MessageType: "world.call", CommandID: 48, RouteKey: protocol.Int64RouteKey(d.Identity.RoleID%32 + 1), Payload: body.Padding[:128]})
				cancel()
				if e != nil {
					return txactor.Result{}, e
				}
				if response.MessageType != "world.done" || !bytes.Equal(response.Payload, body.Padding[:128]) {
					return txactor.Result{}, errors.New("跨 Actor 响应不匹配")
				}
			}
			reply, e := bon.MarshalEnvelope("rank.updated", body)
			if e != nil {
				return txactor.Result{}, e
			}
			if _, e := queue.PubSession(ctx, d.Identity.SessionID, reply); e != nil {
				return txactor.Result{}, e
			}
			return txactor.Result{ProcessedUpSeq: d.UpSeq}, nil
		}
	}
	var dispatch agent.Dispatcher
	if config.Backend == "uplink-proto" {
		kinds := []txactor.Kind{}
		if config.Mode != "agent" {
			kinds = append(kinds, txactor.MustFixedKind(config.Mode, func() protoactor.Actor { return uplinkActor{factory()} }))
		}
		r, e := txactor.New(txactor.Config{ClusterName: config.Namespace, Host: "127.0.0.1", LogLevel: txactor.LogLevelError, MailboxCapacity: 1024, MaxPendingCalls: 4096, Etcd: txactor.EtcdConfig{Endpoints: []string{config.Etcd}, Prefix: "/tx/uplink/" + config.Namespace, DialTimeout: time.Second}}, kinds...)
		if e != nil {
			return nil, e
		}
		if e = r.Start(root); e != nil {
			return nil, e
		}
		closers = append(closers, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if e := r.Drain(ctx); e != nil {
				return e
			}
			return r.Shutdown()
		})
		peer.endpoint, e = r.Endpoint()
		if e != nil {
			return nil, e
		}
		if config.Mode == "game" {
			pid, e := txactor.FixedKindPID(config.World, "world")
			if e != nil {
				return nil, e
			}
			cross = func(ctx context.Context, d txactor.Delivery) (txactor.Result, error) { return r.Call(ctx, pid, d) }
		}
		if config.Mode == "agent" {
			pid, e := txactor.FixedKindPID(config.Target, "game")
			if e != nil {
				return nil, e
			}
			dispatch, e = legacyagent.NewActorDispatcher(legacyagent.ActorDispatcherOptions{Messenger: r, CallTimeout: time.Second, Resolver: legacyagent.ActorDispatchResolverFunc(func(_ context.Context, req agent.DispatchRequest) (legacyagent.ActorDispatch, error) {
				return legacyagent.ActorDispatch{Target: pid, Delivery: txactor.Delivery{MessageType: "game.up", CommandID: uint64(req.CommandID), RouteKey: req.RouteKey, Payload: req.Payload}}, nil
			})})
			if e != nil {
				return nil, e
			}
		}
	} else {
		local := New(1024, time.Second, func(Context) (*Behavior, error) {
			receive := factory()
			return &Behavior{Receive: func(c Context, v any) (any, error) {
				d, e := txactor.DeliveryFromProto(v.(*actorpb.Delivery))
				if e != nil {
					return nil, e
				}
				result, e := receive(c, d)
				return &actorpb.Result{MessageType: result.MessageType, Payload: result.Payload, ProcessedUpSeq: result.ProcessedUpSeq}, e
			}}, nil
		})
		p, e := Connect(config.URL, config.Namespace, config.Namespace+"-"+config.Mode, local, 4096)
		if e != nil {
			return nil, e
		}
		closers = append(closers, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return p.Drain(ctx, nil)
		})
		peer.endpoint = p.instance
		if config.Mode == "game" {
			cross = func(ctx context.Context, d txactor.Delivery) (txactor.Result, error) {
				message, e := d.ToProto()
				if e != nil {
					return txactor.Result{}, e
				}
				key, _ := d.RouteKey.Int64()
				result, e := p.Call(ctx, Target{config.World, Identity{"world", strconv.FormatInt(key, 10)}}, message)
				if e != nil {
					return txactor.Result{}, e
				}
				return txactor.Result{MessageType: result.MessageType, Payload: append([]byte(nil), result.Payload...)}, nil
			}
			if config.Backend == "uplink-hybrid" {
				host, e := rpc.NewHost("127.0.0.1:0")
				if e != nil {
					return nil, e
				}
				closers = append(closers, func() error { return host.Stop(context.Background()) })
				if e = host.Register(func(r grpc.ServiceRegistrar) error {
					return rpc.RegisterDeliveryService(r, nil, NewDirectReceiver(p.instance, local))
				}); e != nil {
					return nil, e
				}
				if e = host.Start(root); e != nil {
					return nil, e
				}
				peer.endpoint = host.Endpoint()
			}
		}
		if config.Mode == "agent" {
			instance := config.Namespace + "-game"
			directory := discovery.NewServiceDirectory()
			if e = directory.Replace([]discovery.Instance{{ID: instance, Endpoint: config.Target, Services: []string{"game"}, Status: discovery.StatusRunning}}); e != nil {
				return nil, e
			}
			resolver, e := discovery.NewSlotResolver(uplinkAssignment{SlotID: "game", RuntimeInstanceID: instance, Generation: 1}, directory)
			if e != nil {
				return nil, e
			}
			var transport Caller = p
			if config.Backend == "uplink-hybrid" {
				dialer, e := rpc.NewGRPCDialer(insecure.NewCredentials(), grpc.WithDisableRetry())
				if e != nil {
					return nil, e
				}
				sender, e := rpc.NewSender(directory, dialer)
				if e != nil {
					return nil, e
				}
				closers = append(closers, sender.Close)
				transport = &DirectTransport{Sender: sender}
			}
			caller := &RoutedCaller{Resolver: resolver, SlotID: func(Identity) string { return "game" }, Transport: transport}
			dispatch = NewAgentDispatcher(caller, func(_ context.Context, req agent.DispatchRequest) (Identity, publicactor.Delivery, error) {
				return Identity{"game", strconv.FormatInt(req.Identity.RoleID, 10)}, publicactor.Delivery{MessageType: "game.up", CommandID: uint64(req.CommandID), RouteKey: req.RouteKey, Payload: req.Payload}, nil
			}, time.Second)
		}
	}
	if config.Mode == "agent" {
		router, e := msgq.NewFixedSessionClientRouter(queue)
		if e != nil {
			return nil, e
		}
		server, e := agent.NewServer(agent.ServerOptions{Dispatcher: dispatch, Sessions: router, IdentityResolver: agent.IdentityResolverFunc(func(_ context.Context, req agent.AuthRequest) (protocol.SessionPrincipal, error) {
			id, e := strconv.ParseInt(string(req.Token), 10, 64)
			return protocol.SessionPrincipal{AccountID: (id-1)/2 + 1, RoleID: (id-1)/2 + 1, ServerID: 1, SessionID: id}, e
		})})
		if e != nil {
			return nil, e
		}
		tcp, e := agent.ListenTCP("127.0.0.1:0", server, 1<<20)
		if e != nil {
			return nil, e
		}
		if e = root.Go(func(ctx context.Context) { _ = tcp.Serve(ctx) }); e != nil {
			return nil, e
		}
		closers = append(closers, func() error { root.Cancel(); return tcp.Close() })
		peer.endpoint = tcp.Addr().String()
	}
	return peer, nil
}
