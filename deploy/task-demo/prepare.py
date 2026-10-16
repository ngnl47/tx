#!/usr/bin/env python3
"""从独立示例的 debug 资产生成 demo 副本，不覆盖现有目录。"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("project", type=Path, help="txgame 或 txgameslg 根目录")
parser.add_argument("output", type=Path, help="新建资产根目录，内部生成 debug")
args = parser.parse_args()
source = args.project / "server/assets/debug"
original = (source / "app.yml").read_text()
# 从配置内容识别布局，不依赖 checkout 文件夹名称。
slg = re.search(r"(?m)^  slgworld:", original) is not None
prefix, subject = ("SLG", "slg") if slg else ("GAME", "game")
match = re.search(r"(?m)^( +)(taskQueue:(?: +&[^\s]+)?) +null *$", original)
if match is None:
    parser.error("输入应为默认关闭 taskQueue 的 debug 资产")
indent = match[1]
queue = {
    "url": "nats://127.0.0.1:14222", "stream": prefix + "_TASKS",
    "subject": subject + ".tasks", "consumer": "workers",
    "deadLetter": subject + ".tasks.dead", "recoveryStream": prefix + "_TASK_EVENTS",
    "recoveryConsumer": "recovery", "timeout": "30s",
}
app = original[:match.start()] + indent + match[2] + "\n" + "\n".join(
    indent + "  " + key + ": " + value for key, value in queue.items()
) + original[match.end():]

# 副本搬离源码目录后，外部 JSON 仍按原 app.yml 定位；保留路径中的发布符号链接。
directory = re.search(r"(?m)^(gameConfig:\n(?: +.*\n)*?  directory: *)([^\n]+)$", app)
if directory is not None:
    value = directory[2].strip()
    if value.startswith('"'):
        value = json.loads(value)
    elif value.startswith("'"):
        value = value[1:-1].replace("''", "'")
    path = os.path.abspath(source / value)
    app = app[:directory.start(2)] + json.dumps(path, ensure_ascii=False) + app[directory.end(2):]
args.output.mkdir(parents=True, exist_ok=False)
target = args.output / "debug"
shutil.copytree(source, target)
(target / "app.yml").write_text(app)
for path in target.glob("*.yml"):
    text = path.read_text()
    for old, new in [(":4222", ":14222"), (":6379", ":16379"), (":27017", ":17017"), (":2379", ":12379")]:
        text = text.replace("127.0.0.1" + old, "127.0.0.1" + new)
    path.write_text(text)

# 存储探针只在 Game 演示副本中启用，普通部署资产不增加测试路由。
if not slg:
    for filename, alias in [("redis.yml", "redis-main"), ("mongodb.yml", "mongo-main")]:
        path = target / filename
        with path.open("a") as output:
            output.write("\nstorageProbe: *" + alias + "\n")
print(args.output.resolve())
