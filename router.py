#!/usr/bin/env python3
"""Local OpenAI-compatible proxy with session-affine llama.cpp slots."""
import argparse
import http.client
import json
import logging
import re
import sqlite3
import threading
import time
from collections import Counter
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

LOG = logging.getLogger("session-slot-router")

HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
       "te", "trailer", "transfer-encoding", "upgrade", "content-length", "host"}


def routing_key(agent, session):
    if not isinstance(agent, str) or agent not in {"pi", "hermes", "opencode"}:
        raise ValueError("agent must be pi, hermes or opencode")
    if not isinstance(session, str) or not session or len(session) > 256:
        raise ValueError("nonempty agent_session_id required (maximum 256 characters)")
    return f"{agent}:{session}"


LOOP_NUDGE = ("Stop: you are repeating the same thoughts in a loop. Do not repeat what you "
              "already thought. First solve a different, smaller sub-step (for example verify "
              "the environment or list the known facts), then return to the original task.")


class LoopDetector:
    """Statistical thinking-loop detector on streamed reasoning_content.

    After --warmup-chars of raw reasoning text, a sliding window of the last
    --window normalized chars (lowercase, digits and CJK only) is checked every
    ~100 new chars: unique char-level --ngram ratio below --ratio means the
    model is re-emitting nearly the same text.
    """

    _CLEAN = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")

    def __init__(self, warmup, window, ngram, ratio):
        self.warmup = warmup
        self.window = window
        self.ngram = ngram
        self.ratio = ratio
        self.raw_len = 0
        self.norm = ""
        self.pending = 0
        self.triggered = False
        self.hit = None

    def feed(self, text):
        """Feed one reasoning_content increment; True when the stream must be cut."""
        if self.triggered:
            return True
        self.raw_len += len(text)
        if self.raw_len < self.warmup:
            return False
        self.norm = (self.norm + self._CLEAN.sub("", text.lower()))[-20000:]
        self.pending += len(text)
        if self.pending < 100:
            return False
        self.pending = 0
        w = self.norm[-self.window:]
        if len(w) < self.window // 2:
            return False
        grams = [w[i:i + self.ngram] for i in range(len(w) - self.ngram + 1)]
        total = len(grams)
        uniq = len(set(grams))
        ratio = uniq / total
        if ratio < self.ratio:
            gram, count = Counter(grams).most_common(1)[0]
            self.triggered = True
            self.hit = f"unique_ratio={ratio:.2f} (<{self.ratio}) top_gram_x{count}: {gram[:60]!r}"
            return True
        return False


class SSEReasoningExtractor:
    """Incrementally pull delta.reasoning_content out of a raw SSE byte stream."""

    def __init__(self):
        self.buf = b""

    def feed(self, data):
        self.buf += data
        out = []
        while b"\n" in self.buf:
            line, self.buf = self.buf.split(b"\n", 1)
            line = line.strip()
            if not line.startswith(b"data:"):
                continue
            payload = line[5:].strip()
            if not payload or payload == b"[DONE]":
                continue
            try:
                obj = json.loads(payload)
            except (UnicodeError, json.JSONDecodeError):
                continue
            try:
                delta = obj["choices"][0].get("delta") or {}
            except (KeyError, IndexError, TypeError):
                continue
            text = delta.get("reasoning_content")
            if isinstance(text, str) and text:
                out.append(text)
        return "".join(out)


def loop_notice(hit, retrying):
    suffix = " Retrying with a corrective instruction." if retrying else ""
    return f"[session-slot-router] response cut: model entered a repetition loop ({hit}).{suffix}"


class Router(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, upstream, database, warmup_chars=4000, window=3000, ngram=18,
                 ratio=0.5, loop_retries=1, nudge=None, loop_cut=True):
        super().__init__(address, Handler)
        self.upstream = urlsplit(upstream)
        if self.upstream.scheme != "http" or not self.upstream.hostname:
            raise ValueError("upstream must be an http:// URL")
        if warmup_chars < 0:
            raise ValueError("warmup_chars must be >= 0")
        if ngram < 2 or window < 2 * ngram:
            raise ValueError("loop detection needs ngram >= 2 and window >= 2*ngram")
        if not 0 < ratio <= 1:
            raise ValueError("loop ratio must be in (0, 1]")
        self.loop_cut = loop_cut
        self.loop_detector = lambda: LoopDetector(warmup_chars, window, ngram, ratio)
        self.loop_retries = max(0, loop_retries)
        if nudge is not None and (not isinstance(nudge, str) or not nudge.strip()):
            raise ValueError("loop nudge must be a nonempty string")
        self.nudge = nudge if nudge is not None else LOOP_NUDGE
        self.db = sqlite3.connect(database, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS assignments (slot INTEGER PRIMARY KEY, routing_key TEXT UNIQUE NOT NULL, last_used INTEGER NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, routing_key TEXT, slot INTEGER, action TEXT NOT NULL, status INTEGER, client_type TEXT)")
        if "client_type" not in {column[1] for column in self.db.execute("PRAGMA table_info(events)")}:
            self.db.execute("ALTER TABLE events ADD COLUMN client_type TEXT")
        self.db.commit()
        self.condition = threading.Condition()
        self.busy = set()
        self.clock = self.db.execute("SELECT COALESCE(MAX(last_used), 0) FROM assignments").fetchone()[0]

    def slots(self):
        with urllib.request.urlopen(f"{self.upstream.scheme}://{self.upstream.netloc}/slots", timeout=5) as response:
            slots = json.load(response)
        if not isinstance(slots, list) or not slots or any(not isinstance(x.get("id"), int) for x in slots):
            raise ValueError("upstream did not return valid slots")
        ids = [x["id"] for x in slots]
        if len(ids) != len(set(ids)):
            raise ValueError("upstream returned duplicate slots")
        return ids

    def acquire(self, key, ids, client_type):
        with self.condition:
            while True:
                rows = self.db.execute("SELECT slot, routing_key, last_used FROM assignments").fetchall()
                assigned = {slot: (name, used) for slot, name, used in rows if slot in ids}
                owned = next((slot for slot, (name, _) in assigned.items() if name == key), None)
                if owned is not None:
                    if owned in self.busy:
                        self.condition.wait()
                        continue
                    slot, action = owned, "reuse"
                else:
                    free = next((slot for slot in ids if slot not in assigned and slot not in self.busy), None)
                    if free is not None:
                        slot, action = free, "assign"
                    else:
                        candidates = [(used, slot) for slot, (_, used) in assigned.items() if slot not in self.busy]
                        if not candidates:
                            self.condition.wait()
                            continue
                        _, slot = min(candidates)
                        action = "evict"
                previous = assigned[slot][0] if action == "evict" else None
                self.clock += 1
                self.db.execute("DELETE FROM assignments WHERE slot = ? OR routing_key = ?", (slot, key))
                self.db.execute("INSERT INTO assignments VALUES (?, ?, ?)", (slot, key, self.clock))
                self.db.execute("INSERT INTO events(at, routing_key, slot, action, client_type) VALUES(?,?,?,?,?)",
                                (int(time.time()), key, slot, action, client_type))
                self.db.commit()
                self.busy.add(slot)
                LOG.info("request session=%r type=%r slot=%d action=%s previous=%r", key, client_type, slot, action, previous)
                return slot

    def loop_event(self, key, slot, action, client_type):
        with self.condition:
            try:
                self.db.execute("INSERT INTO events(at, routing_key, slot, action, client_type) VALUES(?,?,?,?,?)",
                                (int(time.time()), key, slot, action, client_type))
                self.db.commit()
            except sqlite3.Error:
                LOG.exception("could not record %s session=%r slot=%d", action, key, slot)

    def release(self, key, slot, status, completed, client_type):
        with self.condition:
            try:
                self.db.execute("INSERT INTO events(at, routing_key, slot, action, status, client_type) VALUES(?,?,?,?,?,?)",
                                (int(time.time()), key, slot, "response" if completed else "interrupted", status, client_type))
                self.db.commit()
                LOG.info("finished session=%r type=%r slot=%d status=%d result=%s", key, client_type, slot, status,
                         "response" if completed else "interrupted")
            except sqlite3.Error:
                LOG.exception("could not record completion session=%r slot=%d", key, slot)
                raise
            finally:
                self.busy.discard(slot)
                self.condition.notify_all()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except (ConnectionResetError, BrokenPipeError):
            self.close_connection = True

    def error_json(self, status, message):
        LOG.warning("rejected method=%s route=%r status=%d reason=%s", self.command,
                    self.path.split("?", 1)[0][:200], status, message)
        body = json.dumps({"error": {"message": message, "type": "router_error"}}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def read_body(self):
        length = self.headers.get("Content-Length")
        if length is None:
            return b""
        size = int(length)
        if size < 0 or size > 64 * 1024 * 1024:
            raise ValueError("invalid request size")
        return self.rfile.read(size)

    def forward(self):
        try:
            body = self.read_body()
        except (ValueError, OSError) as exc:
            self.error_json(413, str(exc))
            return

        routed = False
        key = None
        client_type = None
        slot = None
        status = 502
        completed = False
        upstream = None
        data = None

        if body:
            try:
                data = json.loads(body)
            except (UnicodeError, json.JSONDecodeError):
                data = None
            if isinstance(data, dict) and "agent_session_id" in data:
                routed = True
                agent = data.pop("agent_name", None)
                session = data.pop("agent_session_id")
                client_type = data.pop("client_type", None)
                supplied_slot = data.get("id_slot")
                LOG.info("incoming method=%s route=%r agent=%r session=%r type=%r requested_slot=%r",
                         self.command, self.path.split("?", 1)[0][:200],
                         agent[:64] if isinstance(agent, str) else None,
                         session[:256] if isinstance(session, str) else None,
                         client_type[:64] if isinstance(client_type, str) else None,
                         supplied_slot if type(supplied_slot) is int and abs(supplied_slot) < 1000000 else None)
                try:
                    key = routing_key(agent, session)
                    if client_type is not None and (not isinstance(client_type, str) or not client_type or len(client_type) > 64):
                        raise ValueError("client_type must be a nonempty string (maximum 64 characters)")
                    ids = self.server.slots()
                    LOG.info("available slots=%s session=%r", ids, key)
                    slot = self.server.acquire(key, ids, client_type)
                except ValueError as exc:
                    self.error_json(400, str(exc))
                    return
                except sqlite3.Error:
                    self.error_json(503, "routing database unavailable")
                    return
                except Exception:
                    self.error_json(502, "cannot discover upstream slots")
                    return
                data["id_slot"] = slot
                body = json.dumps(data, ensure_ascii=False).encode()

        if not routed:
            LOG.info("passthrough method=%s route=%r", self.command, self.path.split("?", 1)[0][:200])

        loop_state = None
        if self.path.split("?", 1)[0].endswith("/chat/completions") and isinstance(data, dict) and data.get("stream"):
            loop_state = {"detector": self.server.loop_detector(), "extractor": SSEReasoningExtractor(), "acted": False}
        try:
            upstream = http.client.HTTPConnection(self.server.upstream.hostname, self.server.upstream.port or 80,
                                                  timeout=1800)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
            if body:
                headers["Content-Length"] = str(len(body))
            target = self.path
            upstream.request(self.command, target, body=body if body else None, headers=headers)
            response = upstream.getresponse()
            status = response.status
            self.send_response_only(status, response.reason)
            chunked = response.getheader("Transfer-Encoding", "").lower() == "chunked"
            content_length = response.getheader("Content-Length")
            for name, value in response.getheaders():
                lower = name.lower()
                if lower in HOP:
                    continue
                self.send_header(name, value)
            if chunked:
                self.send_header("Transfer-Encoding", "chunked")
            elif content_length is not None:
                self.send_header("Content-Length", content_length)
            else:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            cut = False
            if self.command != "HEAD":
                while chunk := response.read1(16384):
                    if loop_state and not loop_state["acted"]:
                        text = loop_state["extractor"].feed(chunk)
                        if text and loop_state["detector"].feed(text):
                            loop_state["acted"] = True
                            LOG.warning("loop detected method=%s route=%r session=%r type=%r slot=%r %s",
                                        self.command, self.path.split("?", 1)[0][:200], key, client_type, slot,
                                        loop_state["detector"].hit)
                            if self.server.loop_cut:
                                if routed:
                                    self.server.loop_event(key, slot, "loop_cut", client_type)
                                if self.server.loop_retries > 0 and isinstance(data.get("messages"), list):
                                    self.write_chunk(self.sse_content(loop_notice(loop_state["detector"].hit, True)), chunked)
                                    if self.loop_retry(key, slot, client_type, routed, data, headers, chunked):
                                        completed = True
                                else:
                                    self.write_chunk(self.sse_content(loop_notice(loop_state["detector"].hit, False)), chunked)
                                    self.write_chunk(self.sse_stop(), chunked)
                                    self.write_chunk(b"data: [DONE]\n\n", chunked)
                                    completed = True
                                cut = True
                                break
                            LOG.info("loop observed (cut disabled), stream continues method=%s route=%r session=%r slot=%r",
                                     self.command, self.path.split("?", 1)[0][:200], key, slot)
                    if chunked:
                        self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    else:
                        self.wfile.write(chunk)
                    self.wfile.flush()
                if chunked:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
            completed = completed or not cut
            if not routed:
                LOG.info("passthrough finished method=%s route=%r status=%d", self.command,
                         self.path.split("?", 1)[0][:200], status)
        except (OSError, http.client.HTTPException):
            if status == 502:
                try:
                    self.error_json(502, "upstream request failed")
                except OSError:
                    pass
        finally:
            if upstream:
                upstream.close()
            if routed and slot is not None:
                self.server.release(key, slot, status, completed, client_type)

    @staticmethod
    def sse_content(text):
        payload = {"id": "session-slot-router", "object": "chat.completion.chunk",
                   "choices": [{"index": 0, "finish_reason": None, "delta": {"content": text}}]}
        return b"data: " + json.dumps(payload).encode() + b"\n\n"

    @staticmethod
    def sse_stop():
        payload = {"id": "session-slot-router", "object": "chat.completion.chunk",
                   "choices": [{"index": 0, "finish_reason": "stop", "delta": {}}]}
        return b"data: " + json.dumps(payload).encode() + b"\n\n"

    def write_chunk(self, payload, chunked):
        if chunked:
            self.wfile.write(f"{len(payload):x}\r\n".encode() + payload + b"\r\n")
        else:
            self.wfile.write(payload)
        self.wfile.flush()

    def loop_retry(self, key, slot, client_type, routed, data, headers, chunked):
        """Cut the looping stream and splice in one retry: same request plus a user nudge,
        forwarded on a second upstream connection. The client sees one continuous response
        with exactly one finish_reason. Returns True when the retry finished normally."""
        retry_body = dict(data)
        retry_body["messages"] = list(data["messages"])
        retry_body["messages"].append({"role": "user", "content": self.server.nudge})
        retry_payload = json.dumps(retry_body, ensure_ascii=False).encode()
        retry_headers = dict(headers)
        retry_headers["Content-Length"] = str(len(retry_payload))
        if routed:
            self.server.loop_event(key, slot, "loop_retry", client_type)
        LOG.info("loop retry method=%s route=%r session=%r type=%r slot=%r",
                 self.command, self.path.split("?", 1)[0][:200], key, client_type, slot)
        upstream = http.client.HTTPConnection(self.server.upstream.hostname, self.server.upstream.port or 80,
                                              timeout=1800)
        try:
            upstream.request(self.command, self.path, body=retry_payload, headers=retry_headers)
            response = upstream.getresponse()
            if response.status != 200:
                LOG.warning("loop retry failed with upstream status=%d method=%s route=%r slot=%r",
                            response.status, self.command, self.path.split("?", 1)[0][:200], slot)
                self.write_chunk(self.sse_content("[session-slot-router] retry failed; no answer available."), chunked)
                self.write_chunk(self.sse_stop(), chunked)
                self.write_chunk(b"data: [DONE]\n\n", chunked)
                return True
            if "text/event-stream" not in (response.getheader("Content-Type") or "").lower():
                LOG.warning("loop retry returned non-SSE content-type=%r method=%s route=%r slot=%r",
                            response.getheader("Content-Type"), self.command, self.path.split("?", 1)[0][:200], slot)
                response.read()
                self.write_chunk(self.sse_content("[session-slot-router] retry failed; no answer available."), chunked)
                self.write_chunk(self.sse_stop(), chunked)
                self.write_chunk(b"data: [DONE]\n\n", chunked)
                return True
            retry_detector = self.server.loop_detector()
            retry_extractor = SSEReasoningExtractor()
            while chunk := response.read1(16384):
                text = retry_extractor.feed(chunk)
                if text and retry_detector.feed(text):
                    LOG.warning("loop detected in retry method=%s route=%r session=%r type=%r slot=%r %s",
                                self.command, self.path.split("?", 1)[0][:200], key, client_type, slot,
                                retry_detector.hit)
                    self.write_chunk(self.sse_content("[session-slot-router] loop persisted after retry; giving up. "
                                                      + (retry_detector.hit or "")), chunked)
                    self.write_chunk(self.sse_stop(), chunked)
                    self.write_chunk(b"data: [DONE]\n\n", chunked)
                    return True
                self.write_chunk(chunk, chunked)
            return True
        except (OSError, http.client.HTTPException):
            LOG.warning("loop retry upstream failed method=%s route=%r slot=%r", self.command,
                        self.path.split("?", 1)[0][:200], slot)
            self.write_chunk(self.sse_stop(), chunked)
            self.write_chunk(b"data: [DONE]\n\n", chunked)
            return True
        finally:
            upstream.close()

    do_GET = forward
    do_HEAD = forward
    do_POST = forward
    do_PUT = forward
    do_PATCH = forward
    do_DELETE = forward
    do_OPTIONS = forward


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--upstream", default="http://127.0.0.1:8080")
    parser.add_argument("--database", default="router.sqlite")
    parser.add_argument("--warmup-chars", type=int, default=4000,
                        help="reasoning chars before loop detection starts (default 4000)")
    parser.add_argument("--window", type=int, default=3000,
                        help="normalized-char sliding window for loop detection (default 3000)")
    parser.add_argument("--ngram", type=int, default=18,
                        help="char-level n-gram length for loop detection (default 18)")
    parser.add_argument("--ratio", type=float, default=0.5,
                        help="unique-ngram ratio below which a loop is detected (default 0.5)")
    parser.add_argument("--loop-nudge", default=None,
                        help="user message injected for the retry (default built-in nudge)")
    parser.add_argument("--loop-retries", type=int, default=1,
                        help="max retries per request after a cut (default 1, 0 = cut only)")
    parser.add_argument("--no-loop-cut", action="store_true",
                        help="observe loops without cutting or retrying")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S%z")
    router = Router((args.host, args.port), args.upstream, args.database, warmup_chars=args.warmup_chars,
                    window=args.window, ngram=args.ngram, ratio=args.ratio, loop_retries=args.loop_retries,
                    nudge=args.loop_nudge, loop_cut=not args.no_loop_cut)
    LOG.info("listening on %s:%d upstream=%s database=%s", args.host, router.server_port, args.upstream, args.database)
    try:
        router.serve_forever()
    finally:
        router.db.close()
        router.server_close()


if __name__ == "__main__":
    main()
