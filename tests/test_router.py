import http.client
import importlib.util
import json
import logging
import io
import re
import subprocess
import sys
import sqlite3
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from router import LOG, Router, routing_key


class UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    calls = []
    paths = []

    def log_message(self, *_):
        pass

    def send_json(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_GET(self):
        self.paths.append((self.command, self.path))
        if self.path == "/slots":
            self.send_json([{"id": 0}, {"id": 1}])
        elif self.path == "/close-delimited":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(b"close-delimited")
            self.close_connection = True
        else:
            self.send_json({"method": self.command, "path": self.path})

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
        self.calls.append(payload)
        self.paths.append((self.command, self.path))
        if payload.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for part in (b'data: {"answer":1}\n\n', b'data: [DONE]\n\n'):
                self.wfile.write(f"{len(part):x}\r\n".encode() + part + b"\r\n")
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
            return
        self.send_json({"slot": payload.get("id_slot"), "payload": payload, "path": self.path})

    do_PUT = do_POST
    do_PATCH = do_POST
    do_DELETE = do_POST
    do_OPTIONS = do_POST
    do_HEAD = do_GET


class RouterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        UpstreamHandler.calls = []
        UpstreamHandler.paths = []
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), UpstreamHandler)
        threading.Thread(target=self.upstream.serve_forever, daemon=True).start()
        self.router = Router(("127.0.0.1", 0), f"http://127.0.0.1:{self.upstream.server_port}", str(Path(self.tmp.name) / "test.sqlite"))
        threading.Thread(target=self.router.serve_forever, daemon=True).start()

    def tearDown(self):
        self.router.shutdown()
        self.router.server_close()
        self.router.db.close()
        self.upstream.shutdown()
        self.upstream.server_close()
        self.tmp.cleanup()

    def ask(self, agent, sid, **extra):
        body = {"agent_name": agent, "agent_session_id": sid, "id_slot": 99, "messages": ["hello"], **extra}
        req = urllib.request.Request(f"http://127.0.0.1:{self.router.server_port}/v1/chat/completions",
                                     json.dumps(body).encode(), {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=4) as r:
            return r.read()

    def test_live_terminal_logging_timestamp(self):
        path = Path(self.tmp.name) / "live.sqlite"
        with subprocess.Popen([sys.executable, "-u", str(ROOT / "router.py"), "--host", "127.0.0.1", "--port", "0",
                               "--upstream", f"http://127.0.0.1:{self.upstream.server_port}", "--database", str(path)],
                              cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True) as process:
            first = process.stderr.readline()
            match = re.search(r"listening on 127\.0\.0\.1:(\d+)", first)
            self.assertIsNotNone(match, first)
            body = {"agent_name": "pi", "agent_session_id": "live", "client_type": "tui", "id_slot": 99,
                    "messages": ["SECRET PROMPT"], "tools": [{"description": "SECRET TOOL"}]}
            request = urllib.request.Request(f"http://127.0.0.1:{match.group(1)}/v1/chat/completions",
                                             json.dumps(body).encode(), {"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=4) as response:
                self.assertEqual(response.status, 200)
                response.read()
            process.terminate()
            remainder = process.communicate(timeout=4)[1]
        output = first + remainder
        self.assertRegex(output, r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{4} INFO listening")
        self.assertIn("incoming method=POST route='/v1/chat/completions' agent='pi' session='live' type='tui' requested_slot=99", output)
        self.assertIn("available slots=[0, 1] session='pi:live'", output)
        self.assertIn("request session='pi:live' type='tui' slot=0 action=assign", output)
        self.assertNotIn("SECRET PROMPT", output)
        self.assertNotIn("SECRET TOOL", output)

    def test_terminal_log_shows_request_result_and_rejection_without_prompts(self):
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                             datefmt="%Y-%m-%d %H:%M:%S%z"))
        LOG.addHandler(handler)
        previous_level = LOG.level
        LOG.setLevel(logging.INFO)
        try:
            self.ask("pi", "log-test", client_type="tui", messages=["SECRET PROMPT"])
            self.ask("hermes", "other", client_type="cli")
            self.ask("pi", "new", client_type="print")
            request = urllib.request.Request(f"http://127.0.0.1:{self.router.server_port}/v1/chat/completions",
                                             b'{"messages":["SECRET BAD REQUEST"]}', {"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=4) as response:
                self.assertEqual(response.status, 200)
                response.read()
        finally:
            LOG.removeHandler(handler)
            LOG.setLevel(previous_level)
        text = output.getvalue()
        self.assertRegex(text, r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{4} INFO request session='pi:log-test' type='tui' slot=0 action=assign")
        self.assertIn("action=evict previous='pi:log-test'", text)
        self.assertIn("finished session='pi:log-test' type='tui' slot=0 status=200 result=response", text)
        self.assertIn("passthrough method=POST route='/v1/chat/completions'", text)
        self.assertNotIn("SECRET PROMPT", text)
        self.assertNotIn("SECRET BAD REQUEST", text)

    def test_arbitrary_route_passes_through_without_logging_body_or_query(self):
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        LOG.addHandler(handler)
        previous_level = LOG.level
        LOG.setLevel(logging.INFO)
        try:
            request = urllib.request.Request(f"http://127.0.0.1:{self.router.server_port}/v1/not-supported?token=SECRET_QUERY",
                                             b'{"messages":["SECRET BODY"]}', {"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=4) as response:
                result = json.load(response)
        finally:
            LOG.removeHandler(handler)
            LOG.setLevel(previous_level)
        self.assertEqual(result["path"], "/v1/not-supported?token=SECRET_QUERY")
        self.assertEqual(UpstreamHandler.calls[-1], {"messages": ["SECRET BODY"]})
        self.assertIn("passthrough method=POST route='/v1/not-supported'", output.getvalue())
        self.assertNotIn("SECRET_QUERY", output.getvalue())
        self.assertNotIn("SECRET BODY", output.getvalue())

    def test_existing_database_migrates_without_losing_events(self):
        path = Path(self.tmp.name) / "old.sqlite"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, routing_key TEXT, slot INTEGER, action TEXT NOT NULL, status INTEGER)")
            db.execute("INSERT INTO events(at, routing_key, slot, action, status) VALUES(1, 'pi:old', 0, 'response', 200)")
        upgraded = Router(("127.0.0.1", 0), f"http://127.0.0.1:{self.upstream.server_port}", str(path))
        try:
            self.assertEqual(upgraded.db.execute("SELECT routing_key, client_type FROM events").fetchall(), [("pi:old", None)])
        finally:
            upgraded.server_close()
            upgraded.db.close()

    def test_missing_session_passes_through_unchanged_without_assignment(self):
        body = {"model": "test", "id_slot": 1, "messages": []}
        request = urllib.request.Request(f"http://127.0.0.1:{self.router.server_port}/v1/chat/completions",
                                         json.dumps(body).encode(), {"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=4) as response:
            result = json.load(response)
        self.assertEqual(result["payload"], body)
        self.assertEqual(UpstreamHandler.calls, [body])
        self.assertEqual(self.router.db.execute("SELECT count(*) FROM assignments").fetchone()[0], 0)

    def test_non_post_methods_pass_through(self):
        for method in ("GET", "HEAD", "PUT", "PATCH", "DELETE", "OPTIONS"):
            body = None if method in {"GET", "HEAD"} else json.dumps({"method": method}).encode()
            request = urllib.request.Request(f"http://127.0.0.1:{self.router.server_port}/probe?x=1",
                                             body, {"Content-Type": "application/json"}, method=method)
            with urllib.request.urlopen(request, timeout=4) as response:
                self.assertEqual(response.status, 200)
                if method != "HEAD":
                    response.read()
        self.assertTrue(all((method, "/probe?x=1") in UpstreamHandler.paths
                            for method in ("GET", "HEAD", "PUT", "PATCH", "DELETE", "OPTIONS")))

    def test_non_chunked_response_preserves_content_length(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.router.server_port, timeout=1)
        try:
            connection.request("GET", "/health")
            response = connection.getresponse()
            body = response.read()
            self.assertEqual(response.getheader("Content-Length"), str(len(body)))
            self.assertEqual(json.loads(body), {"method": "GET", "path": "/health"})
        finally:
            connection.close()

    def test_response_without_length_is_close_delimited(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.router.server_port, timeout=1)
        try:
            connection.request("GET", "/close-delimited")
            response = connection.getresponse()
            self.assertEqual(response.getheader("Connection"), "close")
            self.assertEqual(response.read(), b"close-delimited")
        finally:
            connection.close()

    def test_lru_cron_forwarding_and_stream(self):
        self.assertEqual(json.loads(self.ask("pi", "a", client_type="cli"))["slot"], 0)
        self.assertEqual(json.loads(self.ask("hermes", "b", client_type="cli"))["slot"], 1)
        self.assertEqual(json.loads(self.ask("pi", "a", client_type="print"))["slot"], 0)
        self.assertEqual(json.loads(self.ask("hermes", "c"))["slot"], 1)
        cron_key = "cron:abcdef123456"  # Prepared by the Hermes client, not the router.
        self.assertEqual(json.loads(self.ask("hermes", cron_key, client_type="cronjob"))["slot"], 0)
        self.assertEqual(json.loads(self.ask("hermes", cron_key, client_type="cronjob"))["slot"], 0)
        self.assertIn(b"data: [DONE]", self.ask("hermes", cron_key, stream=True))
        self.assertEqual(json.loads(self.ask("hermes", "cron_abcdef123456_20261007_010101"))["slot"], 1)
        self.assertEqual([x["id_slot"] for x in UpstreamHandler.calls], [0, 1, 0, 1, 0, 0, 0, 1])
        self.assertTrue(all("agent_session_id" not in x and "agent_name" not in x and "client_type" not in x for x in UpstreamHandler.calls))
        types = self.router.db.execute("SELECT client_type FROM events WHERE action != 'response' ORDER BY id").fetchall()
        self.assertEqual([x[0] for x in types], ["cli", "cli", "print", None, "cronjob", "cronjob", None, None])
        rows = self.router.db.execute("SELECT action FROM events WHERE action != 'response' ORDER BY id").fetchall()
        self.assertEqual([x[0] for x in rows], ["assign", "assign", "reuse", "evict", "evict", "reuse", "reuse", "evict"])
        self.assertEqual(routing_key("hermes", "cron_abcdef123456_20261007_010101"),
                         "hermes:cron_abcdef123456_20261007_010101")


if __name__ == "__main__":
    unittest.main()
