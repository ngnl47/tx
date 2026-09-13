"""只在临时 modfile/overlay 内接入完整上行夹具和 txgame 的原始生成包。"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

TX = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--goos', default='linux')
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--wire', action='store_true', help='独立字节诊断构建，禁止用于吞吐比较')
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    modfile = out / 'go.mod'
    shutil.copyfile(TX / 'go.mod', modfile)
    shutil.copyfile(TX / 'go.sum', out / 'go.sum')
    subprocess.run(['go', 'mod', 'edit', '-modfile='+str(modfile),
                    '-require=txgame-server@v0.0.0',
                    '-replace=txgame-server='+str(TX.parent / 'txgame/server')], cwd=TX, check=True)
    source = TX / 'internal/actortrial/comparison_test.go'
    text = source.read_text()
    hook = 'func newComparisonPeer(config comparisonConfig) (*comparisonPeer, error) {'
    assert text.count(hook) == 1
    text = text.replace(hook, hook+'\n if strings.HasPrefix(config.Backend, "uplink-") { return newUplinkPeer(config) }')
    text = text.replace('type comparisonPeer struct {', 'type comparisonPeer struct {\n verify func(int) error')
    text = text.replace('case "snapshot":', 'case "verify":\n if peer.verify==nil { t.Fatal("无持久化核验入口") }; if err=peer.verify(load.Requests);err!=nil { t.Fatal(err) }; emit(map[string]int{"Verified":load.Requests}); continue\n case "snapshot":')
    text = text.replace('"strconv"', '"strconv"\n "strings"')
    if args.wire:
        text = text.replace('"strconv"', '"net"\n "strconv"')
        text = text.replace('case "snapshot":', 'case "wire": emit(map[string]any{"Wire":net.TxTrialWire()}); continue\n case "snapshot":')
    (out / source.name).write_text(text)
    mapping = {str(source): str(out / source.name),
               str(TX / 'internal/actortrial/uplink_process_test.go'): str(HERE / 'uplink_test.go')}
    if args.wire:
        goroot = Path(subprocess.check_output(['go', 'env', 'GOROOT'], text=True).strip())
        for name, needles in [('net.go', [('n, err := c.fd.Read(b)', False), ('n, err := c.fd.Write(b)', True)]),
                              ('writev_unix.go', [('n, err := c.fd.writeBuffers(v)', True)])]:
            original = goroot / 'src/net' / name
            content = original.read_text()
            for needle, write in needles:
                assert content.count(needle) == 1
                content = content.replace(needle, needle+f'\n txTrialCount(c, int64(n), {str(write).lower()})')
            target = out / name
            target.write_text(content)
            mapping[str(original)] = str(target)
        counter = out / 'tx_trial_wire.go'
        counter.write_text('''package net
import "sync"
var txTrialBytes = struct { sync.Mutex; values map[string][2]int64 }{values:make(map[string][2]int64)}
func txTrialCount(c *conn,n int64,write bool) {
 if n<=0 || c.fd.raddr==nil { return }
 key:=c.fd.laddr.String()+"->"+c.fd.raddr.String()
 txTrialBytes.Lock();v:=txTrialBytes.values[key];i:=0;if write{i=1};v[i]+=n;txTrialBytes.values[key]=v;txTrialBytes.Unlock()
}
func TxTrialWire() map[string][2]int64 {
 txTrialBytes.Lock();defer txTrialBytes.Unlock()
 result:=make(map[string][2]int64,len(txTrialBytes.values));for k,v:=range txTrialBytes.values{result[k]=v};return result
}
''')
        mapping[str(goroot / 'src/net/tx_trial_wire.go')] = str(counter)
    overlay = out / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': mapping}, indent=2)+'\n')
    env = os.environ | {'GOOS': args.goos, 'GOARCH': 'amd64', 'CGO_ENABLED': '1' if args.race else '0',
                        'GOCACHE': '/private/tmp/tx-go-cache'}
    command = ['go', 'test', '-mod=readonly', '-modfile='+str(modfile), '-overlay='+str(overlay),
               '-c', './internal/actortrial', '-o', str(out / 'comparison.test')]
    if args.race:
        command.insert(2, '-race')
    subprocess.run(command, cwd=TX, env=env, check=True)


if __name__ == '__main__':
    main()
