#!/usr/bin/env python3
"""Central tracker: it indexes chunks; it never receives file data."""

from __future__ import annotations

import argparse
import socketserver
import threading
import time
from collections import defaultdict

from protocol import recv_json, send_json


class TrackerState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        # file_id -> chunk number -> {"host:port": last_seen}
        self.peers = defaultdict(lambda: defaultdict(dict))

    def announce(self, file_id: str, host: str, port: int, chunks: list[int]) -> None:
        now = time.time()
        peer = f"{host}:{port}"
        with self.lock:
            for chunk in chunks:
                self.peers[file_id][int(chunk)][peer] = now

    def lookup(self, file_id: str) -> dict[str, list[str]]:
        cutoff = time.time() - 90
        with self.lock:
            result = {}
            for chunk, owners in self.peers[file_id].items():
                live = [peer for peer, seen in owners.items() if seen >= cutoff]
                if live:
                    result[str(chunk)] = live
            return result


STATE = TrackerState()


class Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        try:
            request = recv_json(self.request)
            action = request.get("action")
            if action == "announce":
                STATE.announce(request["file_id"], request["host"], int(request["port"]), request["chunks"])
                send_json(self.request, {"ok": True})
            elif action == "lookup":
                send_json(self.request, {"ok": True, "owners": STATE.lookup(request["file_id"])})
            else:
                send_json(self.request, {"ok": False, "error": "unknown action"})
        except Exception as exc:  # Keep a malformed request from killing the server.
            send_json(self.request, {"ok": False, "error": str(exc)})


def main() -> None:
    parser = argparse.ArgumentParser(description="P2P chunk tracker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    with socketserver.ThreadingTCPServer((args.host, args.port), Handler) as server:
        server.daemon_threads = True
        print(f"Tracker listening on {args.host}:{args.port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nTracker stopped.")


if __name__ == "__main__":
    main()
