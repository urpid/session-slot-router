#!/usr/bin/env python3
"""Local OpenAI-compatible proxy with session-affine llama.cpp slots."""
import argparse
import http.client
import json
import logging
import sqlite3
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

LOG = logging.getLogger("session-slot-router")

HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
       "te", "trailer", "transfer-encoding", "upgrade", "content-length", "host"}


def routing_key(agent, session):
    if not isinstance(agent, str) or agent not in {"pi", "hermes"}:
        raise ValueError("agent must be pi or hermes")
    if not isinstance(session, str) or not session or len(session) > 256:
        raise ValueError("nonempty agent_session_id required (maximum 256 characters)")
    return f"{agent}:{session}"


class Router(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, upstream, database):
        super().__init__(address, Handler)
        self.upstream = urlsplit(upstream)
        if self.upstream.scheme != "http" or not self.upstream.hostname:
            raise ValueError("upstream must be an http:// URL")
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
            for name, value in response.getheaders():
                lower = name.lower()
                if lower in HOP:
                    continue
                self.send_header(name, value)
            if chunked:
                self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            if self.command != "HEAD":
                while chunk := response.read1(16384):
                    if chunked:
                        self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    else:
                        self.wfile.write(chunk)
                    self.wfile.flush()
                if chunked:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
            completed = True
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
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S%z")
    router = Router((args.host, args.port), args.upstream, args.database)
    LOG.info("listening on %s:%d upstream=%s database=%s", args.host, router.server_port, args.upstream, args.database)
    try:
        router.serve_forever()
    finally:
        router.db.close()
        router.server_close()


if __name__ == "__main__":
    main()
