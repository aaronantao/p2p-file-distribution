#!/usr/bin/env python3
"""Split a source file into hash-named chunks and write the versioned manifest used by every peer."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import threading
from pathlib import Path


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_write(path: Path, data: bytes) -> None:
    """Write a temporary file, then rename it: readers see the old or the new file, never half of one."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def manifest_id(manifest: dict) -> str:
    """Version-specific ID covering every field a peer relies on, so a damaged manifest is detected."""
    fields = {key: manifest.get(key) for key in ("name", "version", "size", "sha256", "chunk_size", "chunks")}
    return sha256(json.dumps(fields, sort_keys=True).encode())[:16]


def build_manifest(source: Path, out: Path, chunk_size: int, version: int, name: str | None = None) -> dict:
    """Store each chunk as <sha256>.bin, so versions share unchanged chunks and never overwrite each other."""
    out.mkdir(parents=True, exist_ok=True)
    chunks, whole = [], hashlib.sha256()
    with source.open("rb") as handle:
        while data := handle.read(chunk_size):
            digest = sha256(data)
            if not (out / f"{digest}.bin").exists():
                atomic_write(out / f"{digest}.bin", data)
            chunks.append({"index": len(chunks), "size": len(data), "sha256": digest})
            whole.update(data)
    manifest = {
        "name": name or source.name,
        "version": version,
        "size": sum(chunk["size"] for chunk in chunks),
        "sha256": whole.hexdigest(),
        "chunk_size": chunk_size,
        "chunks": chunks,
    }
    manifest["file_id"] = manifest_id(manifest)
    return manifest


def write_manifest(path: Path, manifest: dict) -> None:
    atomic_write(path, json.dumps(manifest, indent=2).encode())


def next_version(path: Path, name: str) -> int:
    try:
        old = json.loads(path.read_text())
    except (OSError, ValueError):
        return 1
    return int(old.get("version", 0)) + 1 if old.get("name") == name else 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("--out", type=Path, default=Path("shared"))
    parser.add_argument("--chunk-size", type=int, default=64 * 1024)
    parser.add_argument("--version", type=int, help="default: one more than the existing manifest in --out")
    args = parser.parse_args()
    path = args.out / "manifest.json"
    version = args.version or next_version(path, args.source.name)
    manifest = build_manifest(args.source, args.out, args.chunk_size, version)
    write_manifest(path, manifest)
    print(f"Created {manifest['name']} v{version}: {len(manifest['chunks'])} chunks and {path}")


if __name__ == "__main__":
    main()
