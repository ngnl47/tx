"""验证演示资产改写，不启动外部设施。"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class PrepareTest(unittest.TestCase):
    def test_current_assets(self):
        for slg, directory in [(False, "../../gen/config/json"),
                               (False, "'../../配置 数据'"),
                               (False, json.dumps("/tmp/config data")),
                               (True, None)]:
            with self.subTest(slg=slg, directory=directory), tempfile.TemporaryDirectory() as root:
                project = Path(root) / "renamed-checkout"
                source = project / "server/assets/debug"
                source.mkdir(parents=True)
                app = "app:\n  taskQueue: &task-queue-config null\nruntime: '#include(runtime.yml)'\n"
                app += "services:\n  " + ("slgworld" if slg else "login") + ": {}\n"
                if directory is not None:
                    app += "gameConfig:\n  directory: " + directory + "\n  edition: beijing\n"
                (source / "app.yml").write_text(app)
                (source / "runtime.yml").write_text("cmods:\n  taskQueue: *task-queue-config\n")
                for name in ["redis.yml", "mongodb.yml"]:
                    (source / name).write_text("{}\n")
                output = Path(root) / "demo"
                command = [sys.executable, str(Path(__file__).with_name("prepare.py")),
                           str(project), str(output)]
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                generated = (output / "debug/app.yml").read_text()
                self.assertIn("taskQueue: &task-queue-config\n", generated)
                self.assertIn("stream: " + ("SLG" if slg else "GAME") + "_TASKS\n", generated)
                self.assertEqual((source / "app.yml").read_text(), app)
                self.assertIn("*task-queue-config", (output / "debug/runtime.yml").read_text())
                self.assertEqual("storageProbe:" in (output / "debug/redis.yml").read_text(), not slg)
                if directory is not None:
                    value = generated.split("  directory: ")[1].splitlines()[0]
                    expected = directory.strip("'\"")
                    if not Path(expected).is_absolute():
                        expected = os.path.abspath(source / expected)
                    self.assertEqual(json.loads(value), expected)
                repeated = subprocess.run(command, capture_output=True, text=True)
                self.assertNotEqual(repeated.returncode, 0)
                self.assertEqual((output / "debug/app.yml").read_text(), generated)


if __name__ == "__main__":
    unittest.main()
