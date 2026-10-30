#!/usr/bin/env python3
"""Split a source file and create the manifest used by every peer."""

import argparse
import hashlib
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("--out", type=Path, default=Path("shared"))
    parser.add_argument("--chunk-size", type=int, default=64 * 1024)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    chunks = []
    with args.source.open("rb") as source:
        index = 0
        while data := source.read(args.chunk_size):
            (args.out / f"chunk-{index:05d}.bin").write_bytes(data)
            chunks.append({"index": index, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            index += 1
    manifest = {"name": args.source.name, "size": args.source.stat().st_size, "chunk_size": args.chunk_size, "chunks": chunks}
    manifest["file_id"] = hashlib.sha256(json.dumps(chunks, sort_keys=True).encode()).hexdigest()[:16]
    path = args.out / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    print(f"Created {len(chunks)} chunks and {path}")


if __name__ == "__main__":
    main()
