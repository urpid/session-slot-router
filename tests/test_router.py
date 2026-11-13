import http.client
import importlib.util
import json
import logging
import io
import random
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

from router import LOG, Router, routing_key, LOOP_NUDGE, LoopDetector, SSEReasoningExtractor

LOOP_PHRASE = "but the CMD is using the system python and I keep saying it "


def quiet_handle_error(request, client_address):
    # Loop tests cut upstream streams early; the resulting resets are expected.
    # socketserver (3.14+) passes the exception via sys.exc_info().
    exc = sys.exc_info()[1]
    if not isinstance(exc, (ConnectionResetError, BrokenPipeError)):
        import traceback
        traceback.print_exception(exc)


class UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    calls = []
    paths = []
    single_slot = threading.Lock()
    first_disconnected = threading.Event()

    def log_message(self, *_):
        pass

    def _sse(self, parts):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        try:
            for part in parts:
                self.wfile.write(f"{len(part):x}\r\n".encode() + part + b"\r\n")
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
        except (ConnectionResetError, BrokenPipeError):
            pass  # The router cuts looping streams early; the remaining writes are discarded.

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
        if self.path.endswith("/chat/completions") and payload.get("stream"):
            messages = payload.get("messages") or []
            first = messages[0].get("content") if messages and isinstance(messages[0], dict) else None
            last = messages[-1].get("content") if messages and isinstance(messages[-1], dict) else None
            loop_chunk = ('data: {"choices":[{"delta":{"reasoning_content":"' + LOOP_PHRASE + '"}}]}\n\n').encode()
            loop_chunks = [loop_chunk] * 120
            if first == "single-slot-loop":
                if last != first:
                    if not self.single_slot.acquire(timeout=3):
                        self.send_json({"error": "original request still holds the slot"}, status=503)
                        return
                    try:
                        self._sse([b'data: {"choices":[{"delta":{"content":"recovered"}}]}\n\n',
                                   b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n', b"data: [DONE]\n\n"])
                    finally:
                        self.single_slot.release()
                    return
                with self.single_slot:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    part = ("data: " + json.dumps({"choices": [{"delta": {"reasoning_content": " ⇒" * 80}}]}) + "\n\n").encode()
                    try:
                        for _ in range(600):
                            self.wfile.write(f"{len(part):x}\r\n".encode() + part + b"\r\n")
                            self.wfile.flush()
                            time.sleep(0.01)
                    except (ConnectionResetError, BrokenPipeError):
                        self.first_disconnected.set()
                return
            if first == "symbol-loop-then-recover" and last != first:
                self._sse([b'data: {"choices":[{"delta":{"content":"recovered"}}]}\n\n',
                           b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n', b"data: [DONE]\n\n"])
                return
            if first == last == "symbol-loop-then-recover":
                parts = [" ⇒" * 80 + " ", "⇒" + " ⇒" * 175]
                chunks = [("data: " + json.dumps({"choices": [{"delta": {"reasoning_content": part}}]}) + "\n\n").encode()
                          for part in parts]
                self._sse(chunks + [b"data: [DONE]\n\n"])
                return
            if first == "loop-then-recover" and last is not None and last != first:
                self._sse([b'data: {"choices":[{"delta":{"reasoning_content":"short fresh thought"}}]}\n\n',
                           b'data: {"choices":[{"delta":{"content":"recovered"}}]}\n\n',
                           b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n',
                           b"data: [DONE]\n\n"])
                return
            if first == last == "loop-then-recover":
                self._sse(loop_chunks + [b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n',
                                         b"data: [DONE]\n\n"])
                return
            if first == "loop-always":
                self._sse(loop_chunks + [b"data: [DONE]\n\n"])
                return
            if first == "loop-retry-500" and last is not None and last != first:
                self.send_json({"error": "boom"}, status=500)
                return
            if first == last == "loop-retry-500":
                self._sse(loop_chunks + [b"data: [DONE]\n\n"])
                return
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
        UpstreamHandler.single_slot = threading.Lock()
        UpstreamHandler.first_disconnected = threading.Event()
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), UpstreamHandler)
        self.upstream.handle_error = quiet_handle_error
        threading.Thread(target=self.upstream.serve_forever, daemon=True).start()
        self.router = Router(("127.0.0.1", 0), f"http://127.0.0.1:{self.upstream.server_port}", str(Path(self.tmp.name) / "test.sqlite"))
        self.router.handle_error = quiet_handle_error
        threading.Thread(target=self.router.serve_forever, daemon=True).start()

    def tearDown(self):
        self.router.shutdown()
        self.router.server_close()
        self.router.db.close()
        self.upstream.shutdown()
        self.upstream.server_close()
        self.tmp.cleanup()

    def _client_stream(self, body, port=None):
        connection = http.client.HTTPConnection("127.0.0.1", port or self.router.server_port, timeout=15)
        try:
            connection.request("POST", "/v1/chat/completions", body=json.dumps(body).encode(),
                               headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    @staticmethod
    def _sse_events(body):
        events = []
        for line in body.split(b"\n"):
            line = line.strip()
            if not line.startswith(b"data:"):
                continue
            payload = line[5:].strip()
            if payload == b"[DONE]":
                events.append("[DONE]")
            elif payload:
                events.append(json.loads(payload))
        return events

    @classmethod
    def _finish_stops(cls, events):
        return [e for e in events if isinstance(e, dict)
                and e.get("choices") and e["choices"][0].get("finish_reason") == "stop"]

    def _loop_request(self, scenario, **extra):
        body = {"agent_name": "pi", "agent_session_id": extra.pop("session", "loop"), "stream": True,
                "messages": [{"role": "user", "content": scenario}], **extra}
        return self._client_stream(body)

    @staticmethod
    def _wait_events(db, routing_key, actions, timeout=5.0):
        """Poll the events table: the client may finish reading the stream before release() commits."""
        deadline = time.time() + timeout
        rows = []
        while time.time() < deadline:
            rows = db.execute("SELECT action FROM events WHERE routing_key=? ORDER BY id", (routing_key,)).fetchall()
            if [r[0] for r in rows] == actions:
                return rows
            time.sleep(0.05)
        raise AssertionError(f"events {actions!r} not recorded for {routing_key!r}, got {[r[0] for r in rows]!r}")

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
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, routing_key TEXT, slot INTEGER, action TEXT NOT NULL, status INTEGER)")
        db.execute("INSERT INTO events(at, routing_key, slot, action, status) VALUES(1, 'pi:old', 0, 'response', 200)")
        db.commit()
        db.close()
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


    def test_loop_detector_triggers_only_on_repetition(self):
        detector = LoopDetector(warmup=1000, window=500, ngram=10, ratio=0.5)
        phrase = "the model keeps repeating this exact sentence again "
        self.assertFalse(detector.feed(phrase * 15))
        self.assertTrue(detector.feed(phrase * 15))
        self.assertTrue(detector.triggered)
        self.assertIn("unique_ratio=", detector.hit)
        self.assertIn("top_gram_x", detector.hit)
        diverse = LoopDetector(warmup=100, window=3000, ngram=18, ratio=0.5)
        random.seed(7)
        words = ("alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima "
                 "mike november oscar papa quebec romeo sierra tango uniform victor "
                 "seven nine three eight four zero one five six two").split()
        index = 0
        for _ in range(300):
            index += 1
            text = " ".join(random.choice(words) for _ in range(20))
            if diverse.feed(text):
                self.fail(f"false positive at step {index}")

    def test_sse_extractor_pulls_only_reasoning_content(self):
        extractor = SSEReasoningExtractor()
        text = extractor.feed(b'data: {"choices":[{"delta":{"reasoning_content":"abc"}}]}\n\n'
                              b'data: {"choices":[{"delta":{"content":"visible"}}]}\n\n')
        self.assertEqual(text, "abc")
        split = extractor.feed(b'data: {"choices":[{"delta":{"reasoning_cont')
        self.assertEqual(split, "")
        rest = extractor.feed(b'ent":"de"}}]}\n\n')
        self.assertEqual(rest, "de")

    def test_short_pattern_repetition_across_chunks(self):
        for pattern in (" ⇒", "abc", "abcdefghijklmnop"):
            with self.subTest(pattern=pattern):
                detector = LoopDetector(warmup=100000, window=3000, ngram=18, ratio=0.5)
                self.assertFalse(detector.feed(pattern * 255))
                self.assertFalse(detector.feed(pattern[:-1]))
                self.assertTrue(detector.feed(pattern[-1:]))
                self.assertIn("repeated_pattern_x256", detector.hit)

    def test_short_patterns_do_not_trigger_on_whitespace_or_normal_formatting(self):
        detector = LoopDetector(warmup=100000, window=3000, ngram=18, ratio=0.5)
        self.assertFalse(detector.feed(" \n" * 1000))
        self.assertFalse(detector.feed("-" * 120 + "\n" + "def example():\n    return 42\n" * 5))
        self.assertFalse(detector.feed("abcdefghijklmnopq" * 256))

    def test_symbol_loop_cuts_and_retries(self):
        status, body = self._loop_request("symbol-loop-then-recover", session="symbol-loop")
        self.assertEqual(status, 200)
        self.assertIn(b"repeated_pattern_x256", body)
        self.assertIn(b"recovered", body)
        events = self._sse_events(body)
        self.assertEqual(events.count("[DONE]"), 1)
        self.assertEqual(len(self._finish_stops(events)), 1)
        calls = [c for c in UpstreamHandler.calls if c.get("stream")]
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["messages"][-1], {"role": "user", "content": LOOP_NUDGE})
        self.assertEqual(calls[1]["id_slot"], calls[0]["id_slot"])
        self._wait_events(self.router.db, "pi:symbol-loop", ["assign", "loop_cut", "loop_retry", "response"])

    def test_retry_waits_for_original_disconnect_on_single_slot(self):
        started = time.monotonic()
        status, body = self._loop_request("single-slot-loop", session="single-slot")
        self.assertEqual(status, 200)
        self.assertTrue(UpstreamHandler.first_disconnected.is_set())
        self.assertIn(b"recovered", body)
        self.assertLess(time.monotonic() - started, 3)
        events = self._sse_events(body)
        self.assertEqual(events.count("[DONE]"), 1)
        self.assertEqual(len(self._finish_stops(events)), 1)
        self._wait_events(self.router.db, "pi:single-slot", ["assign", "loop_cut", "loop_retry", "response"])

    def test_loop_detected_cuts_and_retries_with_one_answer(self):
        status, body = self._loop_request("loop-then-recover", session="loop1")
        self.assertEqual(status, 200)
        self.assertIn(b"response cut: model entered a repetition loop", body)
        self.assertIn(b"recovered", body)
        events = self._sse_events(body)
        self.assertEqual(events.count("[DONE]"), 1)
        self.assertEqual(len(self._finish_stops(events)), 1)
        reasoning = 0
        for event in events:
            if isinstance(event, dict) and event.get("choices"):
                reasoning += len(event["choices"][0].get("delta", {}).get("reasoning_content", ""))
        self.assertGreater(reasoning, 3000)
        calls = [c for c in UpstreamHandler.calls if c.get("stream")]
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["messages"][-1], {"role": "user", "content": LOOP_NUDGE})
        self.assertEqual(calls[1]["messages"][:-1], calls[0]["messages"])
        self.assertEqual(calls[1]["id_slot"], calls[0]["id_slot"])
        self.assertNotIn("agent_session_id", calls[1])
        self._wait_events(self.router.db, "pi:loop1", ["assign", "loop_cut", "loop_retry", "response"])

    def test_loop_in_retry_is_cut_without_second_retry(self):
        status, body = self._loop_request("loop-always", session="loop2")
        self.assertEqual(status, 200)
        self.assertIn(b"loop persisted after retry", body)
        events = self._sse_events(body)
        self.assertEqual(events.count("[DONE]"), 1)
        self.assertEqual(len(self._finish_stops(events)), 1)
        self.assertEqual(len([c for c in UpstreamHandler.calls if c.get("stream")]), 2)
        self._wait_events(self.router.db, "pi:loop2", ["assign", "loop_cut", "loop_retry", "response"])

    def test_loop_detection_applies_to_passthrough_without_session(self):
        before = self.router.db.execute("SELECT count(*) FROM events").fetchone()[0]
        status, body = self._client_stream({"stream": True, "messages": [{"role": "user", "content": "loop-then-recover"}]})
        self.assertEqual(status, 200)
        self.assertIn(b"response cut: model entered a repetition loop", body)
        self.assertIn(b"recovered", body)
        calls = [c for c in UpstreamHandler.calls if c.get("stream")]
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["messages"][-1], {"role": "user", "content": LOOP_NUDGE})
        self.assertEqual(self.router.db.execute("SELECT count(*) FROM events").fetchone()[0], before)

    def test_loop_observe_mode_never_cuts(self):
        observer = Router(("127.0.0.1", 0), f"http://127.0.0.1:{self.upstream.server_port}",
                          str(Path(self.tmp.name) / "observe.sqlite"), loop_cut=False)
        threading.Thread(target=observer.serve_forever, daemon=True).start()
        try:
            status, body = self._client_stream({"agent_name": "pi", "agent_session_id": "loop3", "stream": True,
                                                "messages": [{"role": "user", "content": "loop-then-recover"}]},
                                               port=observer.server_port)
            self.assertEqual(status, 200)
            self.assertNotIn(b"response cut", body)
            self.assertEqual(self._sse_events(body).count("[DONE]"), 1)
            self.assertEqual(len([c for c in UpstreamHandler.calls if c.get("stream")]), 1)
        finally:
            observer.shutdown()
            observer.server_close()
            observer.db.close()

    def test_loop_cut_only_without_retry(self):
        cutter = Router(("127.0.0.1", 0), f"http://127.0.0.1:{self.upstream.server_port}",
                        str(Path(self.tmp.name) / "cutonly.sqlite"), loop_retries=0)
        threading.Thread(target=cutter.serve_forever, daemon=True).start()
        try:
            status, body = self._client_stream({"agent_name": "pi", "agent_session_id": "loop4", "stream": True,
                                                "messages": [{"role": "user", "content": "loop-then-recover"}]},
                                               port=cutter.server_port)
            self.assertEqual(status, 200)
            self.assertIn(b"response cut: model entered a repetition loop", body)
            self.assertNotIn(b"recovered", body)
            events = self._sse_events(body)
            self.assertEqual(events.count("[DONE]"), 1)
            self.assertEqual(len(self._finish_stops(events)), 1)
            self.assertEqual(len([c for c in UpstreamHandler.calls if c.get("stream")]), 1)
            self._wait_events(cutter.db, "pi:loop4", ["assign", "loop_cut", "response"])
        finally:
            cutter.shutdown()
            cutter.server_close()
            cutter.db.close()

    def test_loop_retry_upstream_error_is_reported_to_client(self):
        status, body = self._loop_request("loop-retry-500", session="loop6")
        self.assertEqual(status, 200)
        self.assertIn(b"retry failed; no answer available.", body)
        events = self._sse_events(body)
        self.assertEqual(events.count("[DONE]"), 1)
        self.assertEqual(len(self._finish_stops(events)), 1)
        self.assertEqual(len([c for c in UpstreamHandler.calls if c.get("stream")]), 2)

    def test_loop_parameters_are_validated(self):
        upstream_url = f"http://127.0.0.1:{self.upstream.server_port}"
        db = str(Path(self.tmp.name) / "invalid.sqlite")
        with self.assertRaises(ValueError):
            Router(("127.0.0.1", 0), upstream_url, db, ngram=1)
        with self.assertRaises(ValueError):
            Router(("127.0.0.1", 0), upstream_url, db, ngram=2000)
        with self.assertRaises(ValueError):
            Router(("127.0.0.1", 0), upstream_url, db, ratio=0)
        with self.assertRaises(ValueError):
            Router(("127.0.0.1", 0), upstream_url, db, nudge="")

    def test_loop_retry_uses_custom_nudge(self):
        nudger = Router(("127.0.0.1", 0), f"http://127.0.0.1:{self.upstream.server_port}",
                        str(Path(self.tmp.name) / "nudge.sqlite"), nudge="custom nudge marker")
        threading.Thread(target=nudger.serve_forever, daemon=True).start()
        try:
            status, body = self._client_stream({"agent_name": "pi", "agent_session_id": "loop5", "stream": True,
                                                "messages": [{"role": "user", "content": "loop-then-recover"}]},
                                               port=nudger.server_port)
            self.assertEqual(status, 200)
            self.assertIn(b"recovered", body)
            calls = [c for c in UpstreamHandler.calls if c.get("stream")]
            self.assertEqual(calls[1]["messages"][-1], {"role": "user", "content": "custom nudge marker"})
        finally:
            nudger.shutdown()
            nudger.server_close()
            nudger.db.close()


if __name__ == "__main__":
    unittest.main()
