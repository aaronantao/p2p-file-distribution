# Peer-to-Peer File Distribution System

A compact BitTorrent-style lab project. A tracker holds only chunk availability and the latest version of each file; peer-to-peer TCP connections transfer chunks. Every received chunk is SHA-256 verified, then announced immediately, so a downloader becomes a seeder before its full file is complete. When the source file is updated, every peer is notified and downloads only the changed chunks.

## Run locally

Requires Python 3.10+; no packages are needed. Use three terminals.

```bash
# Terminal 1: tracker
python3 tracker.py

# Prepare a file once (creates shared/ chunks and manifest v1)
python3 make_manifest.py input.pdf --out shared

# Terminal 2: initial seeder; --watch publishes a new version whenever input.pdf is saved
python3 peer.py --manifest shared/manifest.json --chunks shared --listen 127.0.0.1:9101 --seed --watch input.pdf

# Terminal 3: downloader (it serves chunks while downloading)
python3 peer.py --manifest shared/manifest.json --chunks peer-a --listen 127.0.0.1:9102 --output received.pdf
```

Start another downloader on port `9103` with `--chunks peer-b`; it will receive some pieces from peer A as soon as A has them. Compare the result:

```bash
shasum -a 256 input.pdf received.pdf
```

To demonstrate failure recovery, stop the seeder after peer A and B have obtained chunks. The remaining peer will retry and obtain chunks from the other peer(s). Use separate `--chunks` folders to retain partial downloads across restarts.

## Updating the file

Edit and save `input.pdf` while the seeder runs with `--watch`. Once the file has stopped changing for 2 seconds, the seeder splits it again, publishes version 2 to the tracker, and the tracker pushes an `update_available` message to every peer:

```text
📢 Update available: input.pdf v2 (you have v1)
📢 input.pdf updated v1 -> v2: 1 of 10 chunks to download, the rest are reused
Complete: received.pdf v2 (...)
```

Each peer downloads only the chunks whose hash changed, from any peer that already has them, then replaces `received.pdf` in one step. A peer that misses the push (or starts later with the old manifest) learns about the new version from the tracker's reply to its next announce, within 25 seconds.

Without `--watch`, publish an update by stopping the seeder, running `make_manifest.py input.pdf --out shared` again (it picks the next version number), and restarting the seeder.

Chunks are stored as `<sha256>.bin`, so versions share unchanged chunks and can never overwrite each other. Chunk folders made by the earlier `chunk-00000.bin` layout are not compatible; run `make_manifest.py` again.

## Separate machines

For separate machines, distribute only `manifest.json` out of band (for example, with the project files); do **not** copy the seeder's chunk directory. This is needed once: later versions come from the tracker, so an old manifest is enough to join. Use reachable LAN IP addresses in `--tracker` and `--listen`, e.g. `--listen 192.168.1.22:9102`. Open the tracker and peer TCP ports in the host firewall if needed.

### Optional Mininet setup

Run `tracker.py` in one Mininet host and use that host's IP in the other hosts' `--tracker` option. Launch one `peer.py --seed --watch input.pdf` in a second host and downloader peers in the rest. Editing the file on the seeder host shows the `publish` and `update_available` messages. The same TCP messages can then be viewed with `tcpdump` or Wireshark on a Mininet interface.

## Protocol

All messages are one JSON object terminated by a newline. Tracker messages are `announce` (file name, file ID, endpoint, owned chunk indices; the reply carries the latest published version), `lookup` (file ID), `publish` (a new manifest) and `get_manifest` (file name). Peer messages are `get_chunk` (chunk hash; the reply returns base64 payload data) and `update_available` (pushed by the tracker). The manifest holds the file name, version number, whole-file SHA-256 and each chunk's SHA-256; its file ID is a hash of all of those, so a damaged manifest is rejected. The tracker expires endpoints not re-announced for 90 seconds.

## Project report

See [REPORT.md](REPORT.md) for architecture, the update design, test cases, results template, and a five-minute demonstration plan.
