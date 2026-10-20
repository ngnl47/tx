package actor

import (
	"context"
	"fmt"
	txprotocol "github.com/ngnl47/tx/protocol"
)

// MessageContext 是一次邮箱调用的显式上下文。State 归当前激活所有，不得在后台协程中修改。
// Handler 可按服务共享；不能把本上下文或 State 保存到共享 Handler 字段中。
type MessageContext struct {
	context.Context
	Actor     Context
	State     any
	Principal txprotocol.SessionIdentity
	Origin    DeliveryOrigin
	RouteKey  txprotocol.RouteKey
}

// MessageBinding 由生成器构造，作用域与 Call/Tell 独立于业务方法实现。
type MessageBinding struct {
	Name                string
	ClientHash          uint32
	Remote, Local, Call bool
	Invoke              func(MessageContext, []byte) (Result, error)
}

// MessageTable 在服务装配时构造，激活期间只读共享。
type MessageTable struct {
	kind    string
	names   map[string]MessageBinding
	clients map[uint32]MessageBinding
}

// NewMessageTable 仅接受生成器已校验的绑定；重复注册是启动错误。
func NewMessageTable(kind string, bindings []MessageBinding) *MessageTable {
	table := &MessageTable{kind: kind, names: make(map[string]MessageBinding), clients: make(map[uint32]MessageBinding)}
	for _, binding := range bindings {
		if _, ok := table.names[binding.Name]; ok {
			panic("actor: duplicate message binding: " + binding.Name)
		}
		table.names[binding.Name] = binding
		if binding.ClientHash != 0 {
			if _, ok := table.clients[binding.ClientHash]; ok {
				panic("actor: duplicate client binding: " + binding.Name)
			}
			table.clients[binding.ClientHash] = binding
		}
	}
	return table
}

// LocalMessage 仅能经当前激活 Post 投递，不是可跨进程序列化的 Delivery。
type LocalMessage struct {
	name    string
	payload []byte
}

// NewLocalMessage 保存编码快照，避免投递后修改请求影响邮箱消费。
func NewLocalMessage(name string, payload []byte) LocalMessage {
	return LocalMessage{name: name, payload: append([]byte(nil), payload...)}
}

// Dispatch 在调用者的 Actor 邮箱内执行；matched=false 时可交给其他协议表。
func (table *MessageTable) Dispatch(actor Context, state any, message any) (result Result, matched bool, err error) {
	if table == nil {
		return result, false, nil
	}
	ctx := MessageContext{Context: actor.Context, Actor: actor, State: state}
	var binding MessageBinding
	var payload []byte
	var task bool
	switch value := message.(type) {
	case LocalMessage:
		binding, matched = table.names[value.name]
		payload = value.payload
		if matched && !binding.Local {
			return result, true, ErrInvalidDelivery
		}
	case Delivery:
		if value.Origin == OriginClient {
			binding, matched = table.clients[value.CommandHash]
		} else {
			binding, matched = table.names[value.MessageType]
		}
		if !matched {
			return result, false, nil
		}
		ctx.Origin = value.Origin
		ctx.RouteKey = value.RouteKey
		if value.Identity != nil {
			ctx.Principal = *value.Identity
		}
		if value.Origin == OriginClient {
			if binding.ClientHash == 0 || value.Identity == nil {
				return result, true, ErrInvalidDelivery
			}
		} else {
			task = value.Origin == OriginTask
			if !binding.Remote || (binding.Call || task) != value.WantsResult() {
				return result, true, ErrInvalidDelivery
			}
		}
		payload = value.Payload
	default:
		return result, false, nil
	}
	if !matched {
		return result, false, nil
	}
	if actor.Identity.Kind != table.kind {
		return result, true, fmt.Errorf("%w: actor kind does not match message table", ErrInvalidDelivery)
	}
	result, err = binding.Invoke(ctx, payload)
	if err == nil && task && !binding.Call {
		result.Completed = true
	}
	return result, true, err
}
