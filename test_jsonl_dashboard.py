import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from functools import partial
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import urlopen
from unittest.mock import patch

from jsonl_dashboard import Handler, JsonlTail


class TailTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "data.jsonl"
        self.tail = JsonlTail(self.path, max_records=3)

    def append(self, value):
        with self.path.open("ab") as output:
            output.write(value)

    def test_wait_append_partial_utf8_and_bad_rows(self):
        self.assertEqual(self.tail.snapshot({})["state"], "waiting")
        content = '{"status":"成功","score":0.5}\n'.encode()
        split = content.index('成'.encode()) + 1
        self.append(content[:split])
        first = self.tail.snapshot({})
        self.assertEqual(first["total"], 0)
        self.assertTrue(first["pending"])
        self.append(content[split:] + b'bad\n[]\n\n{"score":1e999}\n{"score":NaN}\n')
        result = self.tail.snapshot({})
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["invalid"], 4)
        self.assertEqual(result["rows"][0]["line"], 1)
        self.assertEqual(result["categories"], [("成功", 1)])
        self.assertFalse(result["pending"])
        self.assertEqual(self.tail.snapshot({})["total"], 1)

    def test_retention_filter_and_nested_metrics(self):
        for i in range(5):
            self.append((json.dumps({"id": i, "metrics": {"score": i},
                                     "status": "ok" if i % 2 else "error"}) + "\n").encode())
        result = self.tail.snapshot({"metric": ["metrics.score"], "search": ["error"]})
        self.assertEqual((result["total"], result["retained"], result["matched"]), (5, 3, 2))
        self.assertEqual(result["average"], 3)
        self.assertEqual(result["latest"], 4)
        self.assertEqual([row["data"]["id"] for row in result["rows"]], [4, 2])

    def test_truncate_replace_delete_and_recreate(self):
        self.append(b'{"n":100}\n{"n":200}\n')
        self.tail.snapshot({})
        self.path.write_bytes(b'{"n":1}\n')
        self.assertEqual(self.tail.snapshot({})["total"], 1)
        replacement = self.path.with_suffix(".new")
        replacement.write_bytes(b'{"n":2}\n{"n":3}\n')
        replacement.replace(self.path)
        self.assertEqual(self.tail.snapshot({})["latest"], 3)
        self.path.unlink()
        self.assertEqual(self.tail.snapshot({})["total"], 0)
        self.append(b'{"n":9}\n')
        self.assertEqual(self.tail.snapshot({})["latest"], 9)

    def test_same_size_rewrite_and_truncate_then_grow(self):
        self.append(b'{"n":1}\n')
        self.tail.snapshot({})
        old_mtime = self.path.stat().st_mtime_ns
        self.path.write_bytes(b'{"n":2}\n')
        os.utime(self.path, ns=(old_mtime + 1000000, old_mtime + 1000000))
        self.assertEqual(self.tail.snapshot({})["latest"], 2)
        self.path.write_bytes(b'{"n":3}\n{"n":4}\n')
        self.assertEqual(self.tail.snapshot({})["total"], 2)

    def test_read_budget_and_oversized_line_recovery(self):
        self.append(b'{"a":"' + b'x' * 100 + b'"}\n{"n":4}\n')
        with patch("jsonl_dashboard.READ_BUDGET", 20), patch("jsonl_dashboard.MAX_LINE_BYTES", 30):
            states = [self.tail.snapshot({}) for _ in range(7)]
        self.assertEqual(states[0]["state"], "reading")
        self.assertEqual(states[-1]["invalid"], 1)
        self.assertEqual(states[-1]["latest"], 4)
        self.assertEqual(states[-1]["total"], 1)

    def test_pagination_and_finite_average(self):
        tail = JsonlTail(self.path, max_records=100)
        self.append(b'{"n":1.7e308}\n' * 30)
        result = tail.snapshot({"page": ["999"]})
        self.assertEqual((result["page"], result["pages"], len(result["rows"])), (2, 2, 5))
        self.assertEqual(result["average"], 1.7e308)
        self.assertEqual(tail.snapshot({"page": ["bad"]})["page"], 1)
        json.dumps(result, allow_nan=False)

    def test_http_page_api_and_no_arbitrary_file_serving(self):
        self.append(b'{"n":42}\n')
        server = ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, tail=self.tail))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            with urlopen(base + "/") as response:
                self.assertIn("JSONL 动态看板", response.read().decode())
            with urlopen(base + "/api/snapshot?metric=n") as response:
                self.assertEqual(json.load(response)["latest"], 42)
                self.assertEqual(response.headers["Cache-Control"], "no-store")
            with self.assertRaises(HTTPError) as error:
                urlopen(base + "/../request_llm.py")
            self.assertEqual(error.exception.code, 404)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
