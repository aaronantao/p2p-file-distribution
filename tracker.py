#!/usr/bin/env python3
"""Central tracker: it indexes chunks and announces new versions; it never receives file data."""

from __future__ import annotations

import argparse
import socket
import socketserver
import threading
import time
from collections import defaultdict

from make_manifest import manifest_id
from protocol import recv_json, send_json


class TrackerState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        # file_id -> chunk number -> {"host:port": last_seen}
        self.peers = defaultdict(lambda: defaultdict(dict))
        # file name -> newest published manifest
        self.latest: dict[str, dict] = {}
        # file name -> {"host:port": last_seen}, the peers to notify when that file changes
        self.subscribers = defaultdict(dict)

    def announce(self, name: str | None, file_id: str, host: str, port: int, chunks: list[int]) -> dict:
        now = time.time()
        peer = f"{host}:{port}"
        with self.lock:
            for chunk in chunks:
                self.peers[file_id][int(chunk)][peer] = now
            if not name:
                return {}
            self.subscribers[name][peer] = now
            latest = self.latest.get(name)
        return {"latest_version": latest["version"], "latest_file_id": latest["file_id"]} if latest else {}

    def lookup(self, file_id: str) -> dict[str, list[str]]:
        cutoff = time.time() - 90
        with self.lock:
            result = {}
            for chunk, owners in self.peers[file_id].items():
                live = [peer for peer, seen in owners.items() if seen >= cutoff]
                if live:
                    result[str(chunk)] = live
            return result

    def publish(self, manifest: dict) -> list[str]:
        """Record a newer version and return the live peers that should be told about it."""
        if manifest_id(manifest) != manifest.get("file_id"):
            raise ValueError("manifest does not match its file_id")
        name, version = manifest["name"], int(manifest["version"])
        cutoff = time.time() - 90
        with self.lock:
            current = self.latest.get(name)
            if current and version <= current["version"]:
                raise ValueError(f"v{version} is not newer than the published v{current['version']}")
            self.latest[name] = manifest
            return [peer for peer, seen in self.subscribers[name].items() if seen >= cutoff]

    def manifest(self, name: str) -> dict:
        with self.lock:
            if name not in self.latest:
                raise KeyError(f"no published version of {name}")
            return self.latest[name]


STATE = TrackerState()


def notify(peers: list[str], manifest: dict) -> None:
    """Push update_available to each peer; one that misses it learns on its next announce."""
    message = {"action": "update_available", "name": manifest["name"], "version": manifest["version"], "file_id": manifest["file_id"]}

    def push(peer: str) -> None:
        host, port = peer.rsplit(":", 1)
        try:
            with socket.create_connection((host, int(port)), timeout=2) as conn:
                send_json(conn, message)
                recv_json(conn)
            print(f"Notified {peer} of {manifest['name']} v{manifest['version']}", flush=True)
        except (OSError, ValueError) as exc:
            print(f"Could not notify {peer} ({exc}); it will see the update on its next announce", flush=True)

    for peer in peers:
        threading.Thread(target=push, args=(peer,), daemon=True).start()


class Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        try:
            request = recv_json(self.request)
            action = request.get("action")
            if action == "announce":
                latest = STATE.announce(request.get("name"), request["file_id"], request["host"], int(request["port"]), request["chunks"])
                send_json(self.request, {"ok": True, **latest})
            elif action == "lookup":
                send_json(self.request, {"ok": True, "owners": STATE.lookup(request["file_id"])})
            elif action == "publish":
                manifest = request["manifest"]
                peers = STATE.publish(manifest)
                print(f"Published {manifest['name']} v{manifest['version']} ({manifest['file_id']}); notifying {len(peers)} peer(s)", flush=True)
                notify(peers, manifest)
                send_json(self.request, {"ok": True, "notified": len(peers)})
            elif action == "get_manifest":
                send_json(self.request, {"ok": True, "manifest": STATE.manifest(request["name"])})
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
