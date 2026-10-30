# Peer-to-Peer File Distribution System

A compact BitTorrent-style lab project. A tracker holds only chunk availability; peer-to-peer TCP connections transfer chunks. Every received chunk is SHA-256 verified, then announced immediately, so a downloader becomes a seeder before its full file is complete.

## Run locally

Requires Python 3.10+; no packages are needed. Use three terminals.

```bash
# Terminal 1: tracker
python3 tracker.py

# Prepare a file once (creates shared/ chunks and manifest)
python3 make_manifest.py input.pdf --out shared

# Terminal 2: initial seeder
python3 peer.py --manifest shared/manifest.json --chunks shared --listen 127.0.0.1:9101 --seed

# Terminal 3: downloader (it serves chunks while downloading)
python3 peer.py --manifest shared/manifest.json --chunks peer-a --listen 127.0.0.1:9102 --output received.pdf
```

Start another downloader on port `9103` with `--chunks peer-b`; it will receive some pieces from peer A as soon as A has them. Compare the result:

```bash
shasum -a 256 input.pdf received.pdf
```

To demonstrate failure recovery, stop the seeder after peer A and B have obtained chunks. The remaining peer will retry and obtain chunks from the other peer(s). Use separate `--chunks` folders to retain partial downloads across restarts.

For separate machines, distribute only `manifest.json` out of band (for example, with the project files); do **not** copy the seeder's chunk directory. Use reachable LAN IP addresses in `--tracker` and `--listen`, e.g. `--listen 192.168.1.22:9102`. Open the tracker and peer TCP ports in the host firewall if needed.

### Optional Mininet setup

Run `tracker.py` in one Mininet host and use that host's IP in the other hosts' `--tracker` option. Launch one `peer.py --seed` in a second host and downloader peers in the rest. The same TCP messages can then be viewed with `tcpdump` or Wireshark on a Mininet interface.

## Protocol

All messages are one JSON object terminated by a newline. Tracker messages are `announce` (file ID, endpoint, owned chunk indices) and `lookup` (file ID). Peer messages are `get_chunk` (index); the reply returns base64 payload data. The manifest maps each index to its expected SHA-256 digest. The tracker expires endpoints not re-announced for 90 seconds.

## Project report

See [REPORT.md](REPORT.md) for architecture, test cases, results template, and a five-minute demonstration plan.
