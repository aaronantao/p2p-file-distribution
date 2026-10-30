#!/usr/bin/env python3
"""Peer that downloads verified chunks from many nodes and immediately seeds them."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import random
import socket
import socketserver
import threading
import time
from pathlib import Path

from protocol import recv_json, send_json


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_manifest(path: Path) -> dict:
    manifest = json.loads(path.read_text())
    manifest["path"] = path
    manifest["file_id"] = manifest.get("file_id") or sha256(json.dumps(manifest["chunks"], sort_keys=True).encode())[:16]
    return manifest


def tracker_call(address: str, payload: dict) -> dict:
    host, port = address.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=3) as conn:
        send_json(conn, payload)
        response = recv_json(conn)
    if not response.get("ok"):
        raise RuntimeError(response.get("error", "tracker error"))
    return response


class ChunkStore:
    def __init__(self, folder: Path, manifest: dict) -> None:
        self.folder, self.manifest = folder, manifest
        self.folder.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def path(self, index: int) -> Path:
        return self.folder / f"chunk-{index:05d}.bin"

    def have(self) -> list[int]:
        return [i for i, c in enumerate(self.manifest["chunks"]) if self.path(i).is_file() and sha256(self.path(i).read_bytes()) == c["sha256"]]

    def put(self, index: int, data: bytes) -> None:
        expected = self.manifest["chunks"][index]["sha256"]
        if sha256(data) != expected:
            raise ValueError(f"chunk {index} failed SHA-256 verification")
        with self.lock:
            self.path(index).write_bytes(data)


class PeerHandler(socketserver.BaseRequestHandler):
    store: ChunkStore

    def handle(self) -> None:
        try:
            request = recv_json(self.request)
            index = int(request["chunk"])
            path = self.store.path(index)
            if request.get("action") != "get_chunk" or not path.exists():
                raise FileNotFoundError("requested chunk is unavailable")
            data = path.read_bytes()
            send_json(self.request, {"ok": True, "chunk": index, "data": base64.b64encode(data).decode()})
        except Exception as exc:
            send_json(self.request, {"ok": False, "error": str(exc)})


def start_server(host: str, port: int, store: ChunkStore) -> socketserver.ThreadingTCPServer:
    PeerHandler.store = store
    server = socketserver.ThreadingTCPServer((host, port), PeerHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def announce(tracker: str, host: str, port: int, manifest: dict, store: ChunkStore) -> None:
    tracker_call(tracker, {"action": "announce", "file_id": manifest["file_id"], "host": host, "port": port, "chunks": store.have()})


def fetch(owner: str, index: int) -> bytes:
    host, port = owner.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=5) as conn:
        send_json(conn, {"action": "get_chunk", "chunk": index})
        reply = recv_json(conn)
    if not reply.get("ok"):
        raise RuntimeError(reply.get("error", "peer error"))
    return base64.b64decode(reply["data"])


def assemble(output: Path, store: ChunkStore) -> None:
    with output.open("wb") as destination:
        for index in range(len(store.manifest["chunks"])):
            destination.write(store.path(index).read_bytes())


def main() -> None:
    parser = argparse.ArgumentParser(description="P2P peer")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--chunks", required=True, type=Path, help="peer's persistent chunk directory")
    parser.add_argument("--tracker", default="127.0.0.1:9000")
    parser.add_argument("--listen", default="127.0.0.1:0", help="host:port advertised to tracker")
    parser.add_argument("--seed", action="store_true", help="only serve/announce existing chunks")
    parser.add_argument("--output", type=Path, help="reconstructed output path")
    args = parser.parse_args()
    manifest, store = load_manifest(args.manifest), None
    host, port_text = args.listen.rsplit(":", 1)
    store = ChunkStore(args.chunks, manifest)
    server = start_server(host, int(port_text), store)
    actual_port = server.server_address[1]
    announce(args.tracker, host, actual_port, manifest, store)
    print(f"Peer serving {len(store.have())}/{len(manifest['chunks'])} chunks on {host}:{actual_port}", flush=True)
    if args.seed:
        try:
            while True:
                time.sleep(25)
                announce(args.tracker, host, actual_port, manifest, store)
        except KeyboardInterrupt:
            return
    pending = set(range(len(manifest["chunks"]))) - set(store.have())
    started = time.monotonic()
    while pending:
        owners = tracker_call(args.tracker, {"action": "lookup", "file_id": manifest["file_id"]})["owners"]
        progress = False
        for index in list(pending):
            candidates = owners.get(str(index), [])
            random.shuffle(candidates)
            for owner in candidates:
                try:
                    store.put(index, fetch(owner, index))
                    announce(args.tracker, host, actual_port, manifest, store)  # seed immediately
                    pending.remove(index)
                    print(f"Downloaded and seeded chunk {index}; {len(pending)} remain", flush=True)
                    progress = True
                    break
                except (OSError, ValueError, RuntimeError) as exc:
                    print(f"Chunk {index} from {owner} failed: {exc}", flush=True)
        if not progress:
            print("No live owner for remaining chunks; retrying in 1 second", flush=True)
            time.sleep(1)
    if args.output:
        assemble(args.output, store)
        elapsed = time.monotonic() - started
        size = args.output.stat().st_size
        print(f"Complete: {args.output} ({size} bytes, {size / max(elapsed, .001) / 1024:.1f} KiB/s)", flush=True)
    try:
        while True:
            time.sleep(25)
            announce(args.tracker, host, actual_port, manifest, store)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
