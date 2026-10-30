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
- **Follows updates.** When the original file changes, every peer gets a message, downloads only the changed chunks from the swarm, and rebuilds the new version.

## How it works

### The parts

```text
        announce / lookup / publish / update_available
                 (no file data)
   Peer A  <-------------------------->  Tracker
     |  ^                                 ^
     |  | chunk transfer (TCP)            |
     v  |                                 |
   Peer B  <---- chunk transfer (TCP) -->  Peer C
```

| Part | File | Job |
|---|---|---|
| Manifest builder | `make_manifest.py` | Splits the source file into chunks and writes `manifest.json`: the file name, version, size, whole-file hash, chunk size, each chunk's hash, and a file ID |
| Tracker | `tracker.py` | Keeps a list of which peer has which chunk and the newest version of each file, and tells peers when a new version is published. It **never handles file data**. |
| Peer | `peer.py` | Serves its chunks to others, downloads the chunks it is missing, verifies them, reports them to the tracker, and rebuilds the file. It also follows updates, and on the seeder, `--watch` publishes a new version when the source file is saved |
| Protocol | `protocol.py` | Shared helpers that send and receive one JSON message per line over TCP |

### Step by step

1. **Prepare.** `make_manifest.py` cuts the file into chunks and creates the manifest (version 1). The manifest is the only thing a new peer needs before it starts.
2. **Seed.** The first peer (the *seeder*) starts with every chunk. It tells the tracker: "I am at `host:port` and I have chunks 0..N," and publishes the manifest so the tracker knows the current version.
3. **Look up.** A downloading peer asks the tracker: "Who has which chunk of this file?" The tracker answers with a list of owners for each chunk.
4. **Fetch.** For each missing chunk, the downloader picks an owner at random and sends `get_chunk` over a direct TCP connection. The owner replies with the chunk data, base64-encoded.
5. **Verify and share.** The downloader hashes the chunk and compares it with the manifest. If it matches, the chunk is saved and the peer immediately announces it. From then on, other peers can download that chunk from it.
6. **Recover.** If an owner fails or sends bad data, the downloader tries the next owner. If no live owner exists right now, it waits one second and asks the tracker again.
7. **Finish.** When every chunk is present, the file is rebuilt and the throughput is printed. The peer keeps seeding afterwards.
8. **Update.** When the seeder (run with `--watch`) sees the source file saved, it waits until the file stops changing, splits it again as version N+1 and publishes the new manifest to the tracker. The tracker pushes `update_available` to every peer. Each peer fetches the new manifest, keeps the chunks whose hash is unchanged, downloads only the changed ones from other peers (steps 3–6 again), and replaces its output file in one step. A peer that missed the push finds out from its next announce.

### Keeping updates safe

- **Chunks are stored under their hash** (`<sha256>.bin`), so two versions never overwrite each other and unchanged chunks are shared automatically.
- **Every chunk is checked against the hash for the version being built**, so an old or damaged chunk can't slip into the new file.
- **The manifest's ID is a hash of its contents**, and the manifest carries a whole-file hash that is checked after rebuilding.
- **The output file is written to a temporary file, then renamed**, so it is always the complete old version or the complete new one.
- **The seeder waits for the file to stop changing** before publishing, so a half-saved file is never sent out. If a newer version arrives mid-download, the peer switches to it.

### Messages

| From → to | Action | Fields | Reply |
|---|---|---|---|
| Peer → tracker | `announce` | `name`, `file_id`, `host`, `port`, `chunks` | `{ok: true}` plus the latest published version |
| Peer → tracker | `lookup` | `file_id` | Map from chunk index to a list of owner addresses |
| Seeder → tracker | `publish` | `manifest` | Number of peers notified |
| Peer → tracker | `get_manifest` | `name` | The newest manifest |
| Tracker → peer | `update_available` | `name`, `version`, `file_id` | `{ok: true}` |
| Peer → peer | `get_chunk` | `sha256` | Base64 chunk data, or an error |

Peers re-announce every 25 seconds. The tracker drops any peer it hasn't heard from in 90 seconds, so peers that have left stop being handed out.

### Running it

```bash
python3 tracker.py
python3 make_manifest.py input.pdf --out shared
python3 peer.py --manifest shared/manifest.json --chunks shared --listen 127.0.0.1:9101 --seed --watch input.pdf
python3 peer.py --manifest shared/manifest.json --chunks peer-a --listen 127.0.0.1:9102 --output received.pdf
```

Run each command in its own terminal. Add more downloaders on other ports with their own `--chunks` folders. See `README.md` for running it over a local network or in Mininet.

## Why it is useful

- **No single bottleneck.** With one central server, every download uses that server's bandwidth, so it slows down as more people join. Here every downloader also uploads, so more peers means more total capacity.
- **No single point of failure for data.** The tracker only stores *who has what*. Once chunks have spread, the file is still available even after the original seeder goes offline.
- **Guaranteed integrity.** SHA-256 checks on every chunk mean a corrupted or tampered chunk is caught and thrown away. The rebuilt file is byte-for-byte identical to the original.
- **Efficient for large files.** Chunking makes downloads resumable and lets different chunks come from different peers at the same time.
- **Cheap updates.** When the file changes, peers fetch only the changed chunks, and they fetch them from each other, not from the seeder alone. Editing one part of a large file costs a few chunks, not a full download.
- **Real-world relevance.** The same ideas power BitTorrent, game and OS update delivery (for example, Windows Delivery Optimization), and content distribution across large server fleets.
- **Teaching value.** It shows core networking ideas in about 550 readable lines: client/server versus P2P design, TCP sockets, defining an application-layer protocol, concurrency, soft-state leases, and fault tolerance. It runs on one laptop, across a LAN, or inside Mininet, and you can watch its traffic in Wireshark.

## Current limitations

- One file per manifest. The first manifest has to be shared out of band (copied by hand); later versions come from the tracker.
- The tracker is still a central directory. If it goes down, peers can't discover new owners or hear about updates. When it comes back, the seeder publishes the current version again on its next announce.
- Chunks are requested one at a time per peer, with no parallel requests or "rarest-first" selection.
- There is no encryption or peer authentication, and base64 encoding adds about 33% transfer overhead.
- Anyone who can reach the tracker can publish a new version. Signing manifests with the seeder's key would fix this.
- Chunks have a fixed size, so inserting bytes near the start of a file shifts every later chunk and they all count as changed. Edits that keep the size (or changes near the end) reuse most chunks.
- Old versions' chunks stay in each peer's folder until deleted by hand.

These are good directions for future work: parallel downloads, a distributed hash table (DHT) to remove the tracker, signed manifests, content-defined chunking, and TLS.
