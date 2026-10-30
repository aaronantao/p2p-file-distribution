#!/usr/bin/env python3
"""Peer that downloads verified chunks from many nodes, seeds them at once, and follows file updates."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import random
import socket
import socketserver
import threading
import time
from pathlib import Path

from make_manifest import atomic_write, build_manifest, manifest_id, sha256, write_manifest
from protocol import recv_json, send_json

ANNOUNCE_EVERY = 25


def load_manifest(path: Path) -> dict:
    manifest = json.loads(path.read_text())
    if manifest_id(manifest) != manifest.get("file_id"):
        raise SystemExit(f"{path} is damaged or was made by an older make_manifest.py; run make_manifest.py again")
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
    """Chunks live in <sha256>.bin files, so versions share unchanged chunks and never overwrite each other."""

    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.folder.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.verified = {p.stem for p in folder.glob("*.bin") if sha256(p.read_bytes()) == p.stem}

    def path(self, digest: str) -> Path:
        return self.folder / f"{digest}.bin"

    def has(self, digest: str) -> bool:
        with self.lock:
            return digest in self.verified

    def have(self, manifest: dict) -> list[int]:
        with self.lock:
            return [c["index"] for c in manifest["chunks"] if c["sha256"] in self.verified]

    def read(self, digest: str) -> bytes:
        if not self.has(digest):  # only verified hashes are served, so a request can never name another file
            raise FileNotFoundError("requested chunk is unavailable")
        return self.path(digest).read_bytes()

    def put(self, digest: str, data: bytes) -> None:
        if sha256(data) != digest:
            raise ValueError(f"chunk {digest[:12]} failed SHA-256 verification")
        atomic_write(self.path(digest), data)
        with self.lock:
            self.verified.add(digest)

    def add_built(self, manifest: dict) -> None:
        with self.lock:
            self.verified.update(c["sha256"] for c in manifest["chunks"])


def fetch(owner: str, digest: str) -> bytes:
    host, port = owner.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=5) as conn:
        send_json(conn, {"action": "get_chunk", "sha256": digest})
        reply = recv_json(conn)
    if not reply.get("ok"):
        raise RuntimeError(reply.get("error", "peer error"))
    return base64.b64decode(reply["data"])


class Peer:
    def __init__(self, manifest: dict, store: ChunkStore, tracker: str, host: str, output: Path | None, source: bool) -> None:
        self.manifest, self.store, self.tracker, self.host, self.output = manifest, store, tracker, host, output
        self.source = source  # the seeder that owns the original file and publishes its versions
        self.port = 0
        self.lock = threading.Lock()
        self.update_event = threading.Event()

    def current(self) -> dict:
        with self.lock:
            return self.manifest

    def announce(self) -> None:
        manifest = self.current()
        reply = tracker_call(self.tracker, {
            "action": "announce", "name": manifest["name"], "file_id": manifest["file_id"],
            "host": self.host, "port": self.port, "chunks": self.store.have(manifest),
        })
        latest = reply.get("latest_version", 0)
        if latest > manifest["version"]:
            self.update_event.set()  # fallback for a missed push notification
        elif self.source and latest < manifest["version"]:
            # The tracker is behind (first start, a new version, or the tracker restarted): publish ours.
            notified = tracker_call(self.tracker, {"action": "publish", "manifest": manifest})["notified"]
            print(f"Published {manifest['name']} v{manifest['version']} to the tracker; {notified} peer(s) notified", flush=True)

    def on_update_notice(self, message: dict) -> None:
        manifest = self.current()
        if message.get("name") == manifest["name"] and int(message.get("version", 0)) > manifest["version"]:
            print(f"📢 Update available: {message['name']} v{message['version']} (you have v{manifest['version']})", flush=True)
            self.update_event.set()

    def check_for_update(self) -> bool:
        """Switch to the tracker's newest manifest. Returns True if there was a newer version."""
        current = self.current()
        latest = tracker_call(self.tracker, {"action": "get_manifest", "name": current["name"]})["manifest"]
        if manifest_id(latest) != latest.get("file_id"):
            raise ValueError("tracker sent a damaged manifest")
        if latest["version"] <= current["version"]:
            return False
        missing = sum(1 for c in latest["chunks"] if not self.store.has(c["sha256"]))
        print(f"📢 {latest['name']} updated v{current['version']} -> v{latest['version']}: "
              f"{missing} of {len(latest['chunks'])} chunks to download, the rest are reused", flush=True)
        write_manifest(self.store.folder / "manifest.json", latest)
        with self.lock:
            self.manifest = latest
        self.announce()
        return True

    def download(self) -> bool:
        """Fetch every missing chunk of the current version. Returns False if a newer version arrived first."""
        manifest = self.current()
        pending = {c["index"] for c in manifest["chunks"] if not self.store.has(c["sha256"])}
        started = time.monotonic()
        while pending:
            if self.update_event.is_set():
                print(f"Stopping the v{manifest['version']} download: a newer version is available", flush=True)
                return False
            owners = tracker_call(self.tracker, {"action": "lookup", "file_id": manifest["file_id"]})["owners"]
            progress = False
            for index in sorted(pending):
                if self.update_event.is_set():
                    break
                digest = manifest["chunks"][index]["sha256"]
                candidates = [owner for owner in owners.get(str(index), []) if owner != f"{self.host}:{self.port}"]
                random.shuffle(candidates)
                for owner in candidates:
                    try:
                        self.store.put(digest, fetch(owner, digest))
                        self.announce()  # seed immediately
                        pending.remove(index)
                        print(f"Downloaded and seeded chunk {index} of v{manifest['version']}; {len(pending)} remain", flush=True)
                        progress = True
                        break
                    except (OSError, ValueError, RuntimeError) as exc:
                        print(f"Chunk {index} from {owner} failed: {exc}", flush=True)
            if not progress and not self.update_event.is_set():
                print("No live owner for remaining chunks; retrying in 1 second", flush=True)
                time.sleep(1)
        if self.output:
            self.assemble(manifest, started)
        return True

    def assemble(self, manifest: dict, started: float) -> None:
        """Rebuild into a temporary file, check the whole-file hash, then rename over the old output."""
        tmp = self.output.with_name(f".{self.output.name}.tmp")
        whole = hashlib.sha256()
        with tmp.open("wb") as destination:
            for chunk in manifest["chunks"]:
                data = self.store.read(chunk["sha256"])
                whole.update(data)
                destination.write(data)
        if whole.hexdigest() != manifest["sha256"]:
            tmp.unlink()
            raise ValueError("rebuilt file failed the whole-file SHA-256 check")
        os.replace(tmp, self.output)
        elapsed = time.monotonic() - started
        size = manifest["size"]
        print(f"Complete: {self.output} v{manifest['version']} ({size} bytes, {size / max(elapsed, .001) / 1024:.1f} KiB/s)", flush=True)

    def follow_updates(self) -> None:
        while self.update_event.is_set():
            self.update_event.clear()
            if self.check_for_update():
                self.download()  # stops early if yet another version appears; the loop then picks it up

    def run(self) -> None:
        """Keep seeding: re-announce every 25 seconds and act on update notifications as they arrive."""
        next_announce = time.monotonic() + ANNOUNCE_EVERY
        while True:
            self.update_event.wait(max(0, next_announce - time.monotonic()))
            try:
                self.follow_updates()
                if time.monotonic() >= next_announce:
                    next_announce = time.monotonic() + ANNOUNCE_EVERY
                    self.announce()
            except (OSError, ValueError, RuntimeError, KeyError) as exc:
                print(f"Tracker contact failed: {exc}; retrying later", flush=True)
                time.sleep(1)

    def watch(self, source: Path) -> None:
        """Seeder side: when the source file changes and then stays unchanged for 2 seconds, publish it."""
        published, pending, since = None, None, 0.0
        while True:
            time.sleep(1)
            try:
                stat = source.stat()
            except OSError:
                continue  # some editors briefly remove the file while saving
            state = (stat.st_mtime_ns, stat.st_size)
            if state == published:
                continue
            if state != pending:
                pending, since = state, time.monotonic()
                continue
            if time.monotonic() - since < 2:
                continue  # still settling: never publish a half-saved file
            try:
                if self.publish_file(source, state):
                    published = state
            except (OSError, ValueError, RuntimeError) as exc:
                print(f"Publishing {source} failed: {exc}; will retry", flush=True)
            pending = None

    def publish_file(self, source: Path, state: tuple[int, int]) -> bool:
        current = self.current()
        candidate = build_manifest(source, self.store.folder, current["chunk_size"], current["version"] + 1, current["name"])
        after = source.stat()
        if (after.st_mtime_ns, after.st_size) != state:
            print(f"{source} changed while it was being read; waiting for it to settle", flush=True)
            return False
        if candidate["sha256"] == current["sha256"]:
            return True  # touched but not changed
        old = {c["sha256"] for c in current["chunks"]}
        changed = sum(1 for c in candidate["chunks"] if c["sha256"] not in old)
        print(f"📝 {source} changed: v{candidate['version']} has {changed} new chunk(s) of {len(candidate['chunks'])}", flush=True)
        self.store.add_built(candidate)
        write_manifest(self.store.folder / "manifest.json", candidate)
        with self.lock:
            self.manifest = candidate
        self.announce()  # registers the new chunks, then publishes so notified peers find an owner
        return True


class PeerHandler(socketserver.BaseRequestHandler):
    store: ChunkStore
    peer: Peer

    def handle(self) -> None:
        try:
            request = recv_json(self.request)
            action = request.get("action")
            if action == "get_chunk":
                digest = str(request["sha256"])
                send_json(self.request, {"ok": True, "sha256": digest, "data": base64.b64encode(self.store.read(digest)).decode()})
            elif action == "update_available":
                self.peer.on_update_notice(request)
                send_json(self.request, {"ok": True})
            else:
                raise ValueError("unknown action")
        except Exception as exc:
            send_json(self.request, {"ok": False, "error": str(exc)})


def start_server(host: str, port: int, peer: Peer) -> socketserver.ThreadingTCPServer:
    PeerHandler.store, PeerHandler.peer = peer.store, peer
    server = socketserver.ThreadingTCPServer((host, port), PeerHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="P2P peer")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--chunks", required=True, type=Path, help="peer's persistent chunk directory")
    parser.add_argument("--tracker", default="127.0.0.1:9000")
    parser.add_argument("--listen", default="127.0.0.1:0", help="host:port advertised to tracker")
    parser.add_argument("--seed", action="store_true", help="serve existing chunks and publish this version")
    parser.add_argument("--watch", type=Path, help="with --seed: source file to publish again whenever it changes")
    parser.add_argument("--output", type=Path, help="reconstructed output path, rebuilt after every update")
    args = parser.parse_args()
    if args.watch and not args.seed:
        parser.error("--watch is only for the --seed node that owns the source file")
    manifest = load_manifest(args.manifest)
    host, port_text = args.listen.rsplit(":", 1)
    peer = Peer(manifest, ChunkStore(args.chunks), args.tracker, host, args.output, source=args.seed)
    server = start_server(host, int(port_text), peer)
    peer.port = server.server_address[1]
    peer.announce()
    print(f"Peer serving {len(peer.store.have(manifest))}/{len(manifest['chunks'])} chunks of "
          f"{manifest['name']} v{manifest['version']} on {host}:{peer.port}", flush=True)
    if args.watch:
        threading.Thread(target=peer.watch, args=(args.watch,), daemon=True).start()
        print(f"Watching {args.watch}; saving a change publishes the next version", flush=True)
    try:
        if not args.seed:
            peer.download()
        peer.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
