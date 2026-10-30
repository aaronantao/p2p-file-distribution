# P2P File Distribution System — Mini Project Report

## Problem and objectives

Centralized file servers create a bandwidth bottleneck and a single point of failure. This system divides a file into fixed-size chunks and lets a peer fetch different chunks from multiple nodes. As soon as a peer verifies one chunk it advertises that chunk and can upload it to others. Objectives: distribute work, preserve integrity, tolerate departing peers, and measure transfer behavior.

## Architecture / topology

```text
                 announce / lookup only
   Peer A  <-------------------------->  Tracker
     |  ^                                 ^
     |  | chunk TCP                       |
     v  |                                 |
   Peer B  <------ chunk TCP ------>   Peer C
```

The tracker is a directory, not a relay. It maps `(file_id, chunk_index)` to live peer endpoints. A peer has a persistent chunk folder, a TCP server, and a downloader. Each valid downloaded chunk is added to the local folder and immediately announced to the tracker.

## Message design

| Channel | Request | Key fields | Response |
|---|---|---|---|
| Peer → tracker | `announce` | `file_id`, `host`, `port`, `chunks` | `{ok:true}` |
| Peer → tracker | `lookup` | `file_id` | chunk-index → owners map |
| Peer → peer | `get_chunk` | `chunk` | base64 bytes or error |

Messages are newline-delimited JSON over TCP. A `manifest.json` contains filename, size, chunk size, ordered indices, and SHA-256 hashes. Each peer rejects a chunk whose digest differs from the manifest.

## Implementation

`make_manifest.py` splits the source and creates its manifest. `tracker.py` maintains the availability index with a 90-second lease. `peer.py` exposes local chunks, fetches missing chunks, validates them, announces them, and reconstructs the file. The implementation uses only Python’s standard library, making it easy to run in a lab or Mininet namespace (replace the loopback endpoints with Mininet host IPs).

## Test cases

| Case | Method | Expected result |
|---|---|---|
| Normal transfer | one seeder, one downloader | reconstructed digest matches source |
| Multi-peer seeding | run two downloaders concurrently | logs show chunks served by peers that are still downloading |
| Peer failure | stop initial seeder mid-transfer after another peer has chunks | client retries and completes from remaining peers |
| Corruption | modify a chunk before it is served | receiver reports failed SHA-256 and does not save it |
| Resume | stop and restart downloader with same chunk folder | already valid chunks are reused |

## Metrics and results

Record the final rate printed by the peer and calculate `throughput = file_size / completion_time`. For each run capture: chunk size, peers, source size, completion time, throughput, number of failed attempts, and successful uploads per peer. Repeat with 1, 2, and 3 seeders and plot throughput against number of peers. Expected observation: more sources improve availability and may improve throughput, while tiny chunks increase protocol overhead.

## Five-minute live demonstration

1. Start the tracker and split a small file (30 seconds).
2. Start the initial seeder and show its chunk count (30 seconds).
3. Start peers A and B; point out “Downloaded and seeded” lines (90 seconds).
4. Stop the original seeder after pieces spread; show download completes using A/B (90 seconds).
5. compare SHA-256 values and show the tracker/peer message fields (60 seconds).
6. Present the throughput line, architecture, and each member’s component (60 seconds).
