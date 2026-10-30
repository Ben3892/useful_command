#!/usr/bin/env python3
"""A local, dependency-free dashboard for an append-only JSONL file."""

import argparse
from collections import Counter, deque
from datetime import datetime, timezone
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
from statistics import mean
import threading
from urllib.parse import parse_qs, urlsplit


MAX_LINE_BYTES = 1024 * 1024
READ_BUDGET = 4 * 1024 * 1024
HTML_PATH = Path(__file__).with_name("jsonl_dashboard") / "index.html"


def flatten(record, prefix=""):
    """Expose nested scalar fields using dotted paths; arrays stay in details."""
    result = {}
    for key, value in record.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.update(flatten(value, name))
        elif not isinstance(value, list):
            result[name] = value
    return result


def numeric(value):
    try:
        return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)
    except OverflowError:
        return False


def reject_constant(value):
    raise ValueError(f"Invalid JSON constant: {value}")


class JsonlTail:
    def __init__(self, path, max_records=10000):
        self.path = Path(path).expanduser().resolve()
        self.records = deque(maxlen=max_records)
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.records.clear()
        self.identity = None
        self.offset = 0
        self.mtime_ns = None
        self.anchor = b""
        self.pending = b""
        self.discarding = False
        self.total = self.invalid = self.lines = 0

    def consume(self, fragment, complete):
        if not self.discarding:
            if len(self.pending) + len(fragment) > MAX_LINE_BYTES:
                self.pending = b""
                self.discarding = True
            else:
                self.pending += fragment
        if not complete:
            return
        self.lines += 1
        if self.discarding:
            self.invalid += 1
        elif self.pending.strip():
            try:
                record = json.loads(self.pending.decode("utf-8"), parse_constant=reject_constant)
                if not isinstance(record, dict):
                    raise ValueError("Each line must be a JSON object")
                # Reject overflow such as 1e999, as well as nonstandard NaN/Infinity.
                json.dumps(record, allow_nan=False)
                fields = flatten(record)
            except (ValueError, UnicodeError, RecursionError, OverflowError):
                self.invalid += 1
            else:
                self.total += 1
                self.records.append({"index": self.total, "line": self.lines,
                                     "data": record, "fields": fields})
        self.pending = b""
        self.discarding = False

    def refresh(self):
        try:
            source = self.path.open("rb")
        except FileNotFoundError:
            self.reset()
            return "waiting", 0
        with source:
            stat = os.fstat(source.fileno())
            identity = (stat.st_dev, stat.st_ino)
            changed = self.identity is not None and (
                identity != self.identity or stat.st_size < self.offset or
                (stat.st_size == self.offset and stat.st_mtime_ns != self.mtime_ns)
            )
            if not changed and self.anchor:
                source.seek(self.offset - len(self.anchor))
                changed = source.read(len(self.anchor)) != self.anchor
            if changed:
                self.reset()
            self.identity = identity
            source.seek(self.offset)
            remaining = min(READ_BUDGET, max(0, stat.st_size - self.offset))
            while remaining:
                chunk = source.read(min(65536, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                self.offset += len(chunk)
                parts = chunk.split(b"\n")
                for fragment in parts[:-1]:
                    self.consume(fragment, complete=True)
                self.consume(parts[-1], complete=False)
            source.seek(max(0, self.offset - 256))
            self.anchor = source.read(min(256, self.offset))
            self.mtime_ns = stat.st_mtime_ns
            return "reading" if stat.st_size > self.offset else "ready", stat.st_size

    def snapshot(self, query):
        with self.lock:
            state, size = self.refresh()
            rows = list(self.records)
            numeric_fields = set()
            category_fields = set()
            for row in rows:
                for key, value in row["fields"].items():
                    if numeric(value):
                        numeric_fields.add(key)
                    elif isinstance(value, (str, bool)):
                        category_fields.add(key)
            numeric_fields = sorted(numeric_fields)
            category_fields = sorted(category_fields)
            metric = query.get("metric", [""])[0]
            category = query.get("category", [""])[0]
            if metric not in numeric_fields:
                metric = next(iter(numeric_fields), "")
            if category not in category_fields:
                category = "status" if "status" in category_fields else next(iter(category_fields), "")
            search = query.get("search", [""])[0].casefold()
            matched = [row for row in rows if not search or search in
                       json.dumps(row["data"], ensure_ascii=False).casefold()]
            values = [{"index": row["index"], "value": row["fields"][metric]}
                      for row in matched if numeric(row["fields"].get(metric))]
            counts = Counter(str(row["fields"][category]) for row in matched
                             if category in row["fields"] and
                             isinstance(row["fields"][category], (str, bool)))
            try:
                page = max(1, int(query.get("page", ["1"])[0]))
            except ValueError:
                page = 1
            pages = max(1, (len(matched) + 24) // 25)
            page = min(page, pages)
            start = (page - 1) * 25
            visible = list(reversed(matched))[start:start + 25]
            average = mean(point["value"] for point in values) if values else None
            return {
                "path": str(self.path), "state": state, "size": size,
                "total": self.total, "invalid": self.invalid, "retained": len(rows),
                "pending": bool(self.pending) or self.discarding,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "numeric_fields": numeric_fields, "category_fields": category_fields,
                "metric": metric, "category": category, "average": average,
                "latest": values[-1]["value"] if values else None,
                "trend": values[-120:], "categories": counts.most_common(8),
                "category_unique": len(counts), "matched": len(matched),
                "page": page, "pages": pages,
                "rows": [{"index": row["index"], "line": row["line"], "data": row["data"]}
                         for row in visible],
            }


class Handler(BaseHTTPRequestHandler):
    def __init__(self, *args, tail, **kwargs):
        self.tail = tail
        super().__init__(*args, **kwargs)

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/":
            self.respond(200, HTML_PATH.read_bytes(), "text/html; charset=utf-8")
        elif url.path == "/api/snapshot":
            try:
                payload = self.tail.snapshot(parse_qs(url.query))
                body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
            except (OSError, ValueError, OverflowError) as exc:
                self.respond(500, json.dumps({"error": str(exc)}).encode(), "application/json")
            else:
                self.respond(200, body, "application/json; charset=utf-8")
        else:
            self.respond(404, b"Not found", "text/plain")

    def respond(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def main():
    parser = argparse.ArgumentParser(description="本地 JSONL 动态看板，无需安装第三方依赖")
    parser.add_argument("input", type=Path, help="监听的 JSONL 文件（允许尚未创建）")
    parser.add_argument("--port", type=int, default=8765, help="监听端口，默认 8765")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认仅本机访问")
    parser.add_argument("--max-records", type=int, default=10000, help="保留的最近记录数，默认 10000")
    args = parser.parse_args()
    if args.max_records < 1:
        parser.error("--max-records 必须大于 0")
    if not 0 <= args.port <= 65535:
        parser.error("--port 必须在 0 到 65535 之间")
    tail = JsonlTail(args.input, args.max_records)
    server = ThreadingHTTPServer((args.host, args.port), partial(Handler, tail=tail))
    print(f"JSONL 看板：http://{args.host}:{server.server_port}", flush=True)
    print(f"监听文件：{tail.path}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n看板已停止")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
