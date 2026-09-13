"""生成隔离 Go overlay 和测试二进制；不修改 Transport 或模块缓存。"""
import argparse
import json
import os
from pathlib import Path
import subprocess

TX = Path(__file__).resolve().parents[4]
SOURCE = TX / 'internal/actortrial/nats_baseline_test.go'
SLOTS = TX / 'internal/actortrial/slots_baseline_test.go'


def replace(source, old, new):
    assert source.count(old) == 1, (old, source.count(old))
    return source.replace(old, new)


def sources(limit):
    s = SOURCE.read_text()
    s = replace(s, 'type Transport struct {', 'type Transport struct {\n comparisonBatch *comparisonBatch')
    s = replace(s, '// 不在断线期间缓存 RPC；', f'p.comparisonBatch = newComparisonBatch(p, {limit})\n // 不在断线期间缓存 RPC；')
    s = replace(s, 'func(*nats.Conn) { p.closeRequests() }', 'func(*nats.Conn) { p.comparisonBatch.stop(nil); p.closeRequests() }')
    s = replace(s, 'if err != nil {\n\t\treturn nil, err\n\t}\n\tp.conn = conn', 'if err != nil {\n p.comparisonBatch.stop(nil)\n return nil, err\n }\n p.conn = conn')
    s = replace(s, 'func(msg *nats.Msg) { p.finishRequest(msg.Subject, msg, nil) }', 'p.comparisonBatch.finishReply')
    assert s.count('p.conn.PublishMsg(msg)') == 2
    s = s.replace('p.conn.PublishMsg(msg)', 'p.comparisonPublish(msg)')
    assert s.count('delete(p.pending, reply)\n\t\tcall.stop()') == 2
    s = s.replace('delete(p.pending, reply)\n\t\tcall.stop()', 'delete(p.pending, reply)\n if p.comparisonBatch != nil { p.comparisonBatch.removeLocked(reply) }\n call.stop()')
    s = replace(s, 'func (p *Transport) receive(msg *nats.Msg) {', 'func (p *Transport) receive(msg *nats.Msg) {\n if msg.Header.Get(comparisonBatchHeader) != "" { p.comparisonBatch.receive(msg); return }')
    s = replace(s, '// 此时 worker 已发完结果和卸载前 RPC；', 'if p.comparisonBatch != nil { if err := p.comparisonBatch.stop(ctx); err != nil { return err } }\n // 此时 worker 已发完结果和卸载前 RPC；')
    s += '''
// 协议层夹具手工构造 Transport；控制报文不进入业务发送队列。
func (p *Transport) comparisonPublish(msg *nats.Msg) error {
 if p.comparisonBatch == nil || msg.Header.Get("Actor-Identity") == "" || msg.Header.Get("Actor-Control") != "" { return p.conn.PublishMsg(msg) }
 return p.comparisonBatch.send(msg)
}
'''
    slots = replace(SLOTS.read_text(), 'if err := h.conn.FlushWithContext(ctx); err != nil {', 'if err := h.comparisonBatch.stop(ctx); err != nil { return err }\n if err := h.conn.FlushWithContext(ctx); err != nil {')
    return {SOURCE:s, SLOTS:slots}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--limit', type=int, choices=(1,64), default=64)
    parser.add_argument('--overlay-only', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    mapping = {}
    for source, text in sources(args.limit).items():
        target=args.output/source.name
        target.write_text(text)
        subprocess.run(['gofmt','-w',str(target)],check=True)
        mapping[str(source)]=str(target)
    overlay=args.output/'overlay.json'
    overlay.write_text(json.dumps({'Replace':mapping},indent=2)+'\n')
    if not args.overlay_only:
        env=os.environ | {'GOOS':'linux','GOARCH':'amd64','CGO_ENABLED':'0','GOCACHE':'/private/tmp/tx-go-cache'}
        subprocess.run(['go','test','-mod=readonly','-tags=tx_nats_batch','-overlay='+str(overlay),'-c','./internal/actortrial','-o',str(args.output/'comparison.test')],cwd=TX,env=env,check=True)


if __name__ == '__main__':
    main()
