package util

import (
	"fmt"
	"reflect"
	"sync"
)

// BusSubscriber 定义事件总线的订阅相关行为。
type BusSubscriber interface {
	Subscribe(topic string, fn interface{}) error
	SubscribeAsync(topic string, fn interface{}, transactional bool) error
	SubscribeOnce(topic string, fn interface{}) error
	SubscribeOnceAsync(topic string, fn interface{}) error
	Unsubscribe(topic string, handler interface{}) error
}

// BusPublisher 定义事件总线的发布相关行为。
type BusPublisher interface {
	Publish(topic string, args ...interface{})
}

// BusController 定义事件总线的控制行为，例如检查回调是否存在和等待同步。
type BusController interface {
	HasCallback(topic string) bool
	WaitAsync()
}

// Bus 聚合订阅、发布和控制行为。
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
}

// EventBus 保存事件处理器和回调。
type EventBus struct {
	handlers map[string][]*eventHandler
	mu       sync.RWMutex
	wg       sync.WaitGroup
}

type eventHandler struct {
	callBack      reflect.Value
	flagOnce      bool
	async         bool
	transactional bool
	once          sync.Once // SubscribeOnce(Async)：用户回调最多执行一次。
	sync.Mutex              // transactional async：串行化该订阅的回调。
}

// NewEventBus 创建一个不含任何处理器的新 EventBus。
func NewEventBus() Bus {
	b := &EventBus{
		handlers: make(map[string][]*eventHandler),
	}
	return Bus(b)
}

// doSubscribe 处理订阅逻辑，供公开 Subscribe 方法复用。
func (bus *EventBus) doSubscribe(topic string, fn interface{}, handler *eventHandler) error {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if reflect.TypeOf(fn).Kind() != reflect.Func {
		return fmt.Errorf("%s is not of type reflect.Func", reflect.TypeOf(fn).Kind())
	}
	bus.handlers[topic] = append(bus.handlers[topic], handler)
	return nil
}

// Subscribe 订阅指定 topic。
// 如果 fn 不是函数则返回错误。
func (bus *EventBus) Subscribe(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{callBack: reflect.ValueOf(fn)})
}

// SubscribeAsync 以异步回调订阅指定 topic。
// transactional 决定同一 topic 的后续回调是串行执行（true）还是并发执行（false）。
// 如果 fn 不是函数则返回错误。
func (bus *EventBus) SubscribeAsync(topic string, fn interface{}, transactional bool) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack:      reflect.ValueOf(fn),
		async:         true,
		transactional: transactional,
	})
}

// SubscribeOnce 只订阅指定 topic 一次，处理器执行后会被移除。
// 如果 fn 不是函数则返回错误。
func (bus *EventBus) SubscribeOnce(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack: reflect.ValueOf(fn),
		flagOnce: true,
	})
}

// SubscribeOnceAsync 以异步回调只订阅指定 topic 一次。
// 处理器执行后会被移除。如果 fn 不是函数则返回错误。
func (bus *EventBus) SubscribeOnceAsync(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack: reflect.ValueOf(fn),
		flagOnce: true,
		async:    true,
	})
}

// HasCallback 判断指定 topic 是否存在已订阅回调。
func (bus *EventBus) HasCallback(topic string) bool {
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	h, ok := bus.handlers[topic]
	return ok && len(h) > 0
}

// Unsubscribe 移除指定 topic 上的回调。
// 如果该 topic 没有已订阅回调则返回错误。
func (bus *EventBus) Unsubscribe(topic string, handler interface{}) error {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if _, ok := bus.handlers[topic]; ok && len(bus.handlers[topic]) > 0 {
		bus.removeHandler(topic, bus.findHandlerIdx(topic, reflect.ValueOf(handler)))
		return nil
	}
	return fmt.Errorf("topic %s doesn't exist", topic)
}

// Publish 执行指定 topic 上的回调，并把额外参数传给回调函数。
//
// 处理器会在短暂读锁内解析成快照；回调执行时不持有总线锁，因此并发 Publish
// 和慢回调不会阻塞其他 topic。Unsubscribe 可能与 Publish 竞争：快照创建后
// 被移除的处理器仍可能再执行一次。
func (bus *EventBus) Publish(topic string, args ...interface{}) {
	bus.mu.RLock()
	handlers, ok := bus.handlers[topic]
	if !ok || len(handlers) == 0 {
		bus.mu.RUnlock()
		return
	}
	copyHandlers := append([]*eventHandler(nil), handlers...)
	bus.mu.RUnlock()

	for _, handler := range copyHandlers {
		if handler.async {
			bus.wg.Add(1)
			if handler.transactional {
				// 对同一个 transactional 订阅者串行化连续 Publish：
				// 启动 worker 前在发布 goroutine 上加锁，以匹配旧 EventBus 行为。
				handler.Lock()
				go bus.runTransactionalAsync(handler, topic, args...)
				continue
			}
			go bus.doPublishAsync(handler, topic, args...)
			continue
		}
		if handler.flagOnce {
			h := handler
			h.once.Do(func() {
				bus.mu.Lock()
				bus.removeHandlerIfPresent(topic, h)
				bus.mu.Unlock()
				bus.doPublish(h, topic, args...)
			})
			continue
		}
		bus.doPublish(handler, topic, args...)
	}
}

func (bus *EventBus) doPublish(handler *eventHandler, topic string, args ...interface{}) {
	passedArguments := bus.setUpPublish(handler, args...)
	handler.callBack.Call(passedArguments)
}

func (bus *EventBus) doPublishAsync(handler *eventHandler, topic string, args ...interface{}) {
	defer bus.wg.Done()
	if handler.flagOnce {
		handler.once.Do(func() {
			bus.mu.Lock()
			bus.removeHandlerIfPresent(topic, handler)
			bus.mu.Unlock()
			bus.doPublish(handler, topic, args...)
		})
		return
	}
	bus.doPublish(handler, topic, args...)
}

// runTransactionalAsync 在发布方获取 handler.Lock() 后运行。
func (bus *EventBus) runTransactionalAsync(handler *eventHandler, topic string, args ...interface{}) {
	defer bus.wg.Done()
	defer handler.Unlock()
	if handler.flagOnce {
		handler.once.Do(func() {
			bus.mu.Lock()
			bus.removeHandlerIfPresent(topic, handler)
			bus.mu.Unlock()
			bus.doPublish(handler, topic, args...)
		})
		return
	}
	bus.doPublish(handler, topic, args...)
}

// removeHandlerIfPresent 从 bus.handlers[topic] 中移除 h。
// 调用方必须持有 bus.mu 写锁。
func (bus *EventBus) removeHandlerIfPresent(topic string, h *eventHandler) {
	slice, ok := bus.handlers[topic]
	if !ok {
		return
	}
	for i, eh := range slice {
		if eh == h {
			bus.removeHandler(topic, i)
			return
		}
	}
}

func (bus *EventBus) removeHandler(topic string, idx int) {
	if _, ok := bus.handlers[topic]; !ok {
		return
	}
	l := len(bus.handlers[topic])

	if idx < 0 || idx >= l {
		return
	}

	copy(bus.handlers[topic][idx:], bus.handlers[topic][idx+1:])
	bus.handlers[topic][l-1] = nil
	bus.handlers[topic] = bus.handlers[topic][:l-1]
}

func (bus *EventBus) findHandlerIdx(topic string, callback reflect.Value) int {
	if _, ok := bus.handlers[topic]; ok {
		for idx, handler := range bus.handlers[topic] {
			if handler.callBack.Type() == callback.Type() &&
				handler.callBack.Pointer() == callback.Pointer() {
				return idx
			}
		}
	}
	return -1
}

func (bus *EventBus) setUpPublish(callback *eventHandler, args ...interface{}) []reflect.Value {
	funcType := callback.callBack.Type()
	passedArguments := make([]reflect.Value, len(args))
	for i, v := range args {
		if v == nil {
			passedArguments[i] = reflect.New(funcType.In(i)).Elem()
		} else {
			passedArguments[i] = reflect.ValueOf(v)
		}
	}

	return passedArguments
}

// WaitAsync 等待所有异步回调执行完成。
func (bus *EventBus) WaitAsync() {
	bus.wg.Wait()
}
