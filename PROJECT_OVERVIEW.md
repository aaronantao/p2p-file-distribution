# Project Overview: Peer-to-Peer File Distribution System

## What it is

This project is a small peer-to-peer (P2P) file sharing system, modelled on how BitTorrent works. A file is split into chunks. Computers on the network (called **peers**) download those chunks directly from each other instead of all downloading from one central server.

It is written in plain Python 3.10+ and uses only the standard library, so there is nothing to install. It was built as the mini-project for the Computer Networks Laboratory (LCA 3).

## What it does

- **Splits a file into chunks.** By default each chunk is 64 KB. Each chunk gets a SHA-256 fingerprint (hash).
- **Downloads chunks from several peers at once.** A peer can get different chunks from different computers.
- **Checks every chunk.** A chunk that doesn't match its expected hash is rejected and never saved.
- **Shares chunks right away.** As soon as a peer has a verified chunk, it tells the tracker and starts serving that chunk to others, even before its own download is finished.
- **Survives peers leaving.** If a peer goes offline, the downloader retries with the remaining peers that have the chunk.
- **Resumes downloads.** Chunks are saved to a folder for each peer, so a restarted download only fetches what is missing.
- **Rebuilds the original file.** Once all chunks arrive, they are joined back together and the speed is reported.

## How it works

### The parts

```text
                 announce / lookup only
   Peer A  <-------------------------->  Tracker
     |  ^                                 ^
     |  | chunk transfer (TCP)            |
     v  |                                 |
   Peer B  <---- chunk transfer (TCP) -->  Peer C
```

| Part | File | Job |
|---|---|---|
| Manifest builder | `make_manifest.py` | Splits the source file into chunks and writes `manifest.json`: the file name, size, chunk size, each chunk's hash, and a file ID |
| Tracker | `tracker.py` | Keeps a list of which peer has which chunk. It **never handles file data**. |
| Peer | `peer.py` | Serves its chunks to others, downloads the chunks it is missing, verifies them, reports them to the tracker, and rebuilds the file |
| Protocol | `protocol.py` | Shared helpers that send and receive one JSON message per line over TCP |

### Step by step

1. **Prepare.** `make_manifest.py` cuts the file into chunks and creates the manifest. The manifest is the only thing a new peer needs before it starts.
2. **Seed.** The first peer (the *seeder*) starts with every chunk. It tells the tracker: "I am at `host:port` and I have chunks 0..N."
3. **Look up.** A downloading peer asks the tracker: "Who has which chunk of this file?" The tracker answers with a list of owners for each chunk.
4. **Fetch.** For each missing chunk, the downloader picks an owner at random and sends `get_chunk` over a direct TCP connection. The owner replies with the chunk data, base64-encoded.
5. **Verify and share.** The downloader hashes the chunk and compares it with the manifest. If it matches, the chunk is saved and the peer immediately announces it. From then on, other peers can download that chunk from it.
6. **Recover.** If an owner fails or sends bad data, the downloader tries the next owner. If no live owner exists right now, it waits one second and asks the tracker again.
7. **Finish.** When every chunk is present, the file is rebuilt and the throughput is printed. The peer keeps seeding afterwards.

### Messages

| From → to | Action | Fields | Reply |
|---|---|---|---|
| Peer → tracker | `announce` | `file_id`, `host`, `port`, `chunks` | `{ok: true}` |
| Peer → tracker | `lookup` | `file_id` | Map from chunk index to a list of owner addresses |
| Peer → peer | `get_chunk` | `chunk` | Base64 chunk data, or an error |

Peers re-announce every 25 seconds. The tracker drops any peer it hasn't heard from in 90 seconds, so peers that have left stop being handed out.

### Running it

```bash
python3 tracker.py
python3 make_manifest.py input.pdf --out shared
python3 peer.py --manifest shared/manifest.json --chunks shared --listen 127.0.0.1:9101 --seed
python3 peer.py --manifest shared/manifest.json --chunks peer-a --listen 127.0.0.1:9102 --output received.pdf
```

Run each command in its own terminal. Add more downloaders on other ports with their own `--chunks` folders. See `README.md` for running it over a local network or in Mininet.

## Why it is useful

- **No single bottleneck.** With one central server, every download uses that server's bandwidth, so it slows down as more people join. Here every downloader also uploads, so more peers means more total capacity.
- **No single point of failure for data.** The tracker only stores *who has what*. Once chunks have spread, the file is still available even after the original seeder goes offline.
- **Guaranteed integrity.** SHA-256 checks on every chunk mean a corrupted or tampered chunk is caught and thrown away. The rebuilt file is byte-for-byte identical to the original.
- **Efficient for large files.** Chunking makes downloads resumable and lets different chunks come from different peers at the same time.
- **Real-world relevance.** The same ideas power BitTorrent, game and OS update delivery (for example, Windows Delivery Optimization), and content distribution across large server fleets.
- **Teaching value.** It shows core networking ideas in about 300 readable lines: client/server versus P2P design, TCP sockets, defining an application-layer protocol, concurrency, soft-state leases, and fault tolerance. It runs on one laptop, across a LAN, or inside Mininet, and you can watch its traffic in Wireshark.

## Current limitations

- One file per manifest. The manifest has to be shared out of band (copied by hand).
- The tracker is still a central directory. If it goes down, peers can't discover new owners.
- Chunks are requested one at a time per peer, with no parallel requests or "rarest-first" selection.
- There is no encryption or peer authentication, and base64 encoding adds about 33% transfer overhead.

These are good directions for future work: parallel downloads, a distributed hash table (DHT) to remove the tracker, and TLS.
