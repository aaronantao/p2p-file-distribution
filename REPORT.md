# P2P File Distribution System — Mini Project Report

## Problem and objectives

Centralized file servers create a bandwidth bottleneck and a single point of failure. This system divides a file into fixed-size chunks and lets a peer fetch different chunks from multiple nodes. As soon as a peer verifies one chunk it advertises that chunk and can upload it to others. When the source file is updated, the tracker only notifies peers; the new version is distributed the same way as the original, with unchanged chunks reused. Objectives: distribute work, preserve integrity, tolerate departing peers, keep every node on the latest version, and measure transfer behavior.

## Architecture / topology

```text
        announce / lookup / publish / update_available
                 (no file data)
   Peer A  <-------------------------->  Tracker
     |  ^                                 ^
     |  | chunk TCP                       |
     v  |                                 |
   Peer B  <------ chunk TCP ------>   Peer C
```

The tracker is a directory, not a relay. It maps `(file_id, chunk_index)` to live peer endpoints, keeps the newest manifest for each file name, and remembers which peers follow each file so it can notify them. A peer has a persistent chunk folder, a TCP server (which also receives update notifications), and a downloader. Each valid downloaded chunk is added to the local folder and immediately announced to the tracker.

## Message design

| Channel | Request | Key fields | Response |
|---|---|---|---|
| Peer → tracker | `announce` | `name`, `file_id`, `host`, `port`, `chunks` | `{ok:true}` + latest version |
| Peer → tracker | `lookup` | `file_id` | chunk-index → owners map |
| Seeder → tracker | `publish` | `manifest` | peers notified |
| Peer → tracker | `get_manifest` | `name` | newest manifest |
| Tracker → peer | `update_available` | `name`, `version`, `file_id` | `{ok:true}` |
| Peer → peer | `get_chunk` | `sha256` | base64 bytes or error |

Messages are newline-delimited JSON over TCP. A `manifest.json` contains filename, version, size, whole-file SHA-256, chunk size, ordered indices, and per-chunk SHA-256 hashes; its `file_id` is a hash of those fields. Each peer rejects a chunk whose digest differs from the manifest of the version it is building.

## Update notification

The seeder runs with `--watch`. When the source file has been saved and left unchanged for 2 seconds, it builds manifest version N+1 and sends `publish` to the tracker. The tracker checks the manifest's ID, records it as the latest version, and pushes `update_available` to every live peer. A peer fetches the new manifest with `get_manifest`, reuses chunks whose hash is unchanged, downloads the rest from other peers, verifies the whole-file hash, and renames the rebuilt file over the old one. Peers that miss the push see the newer version in the reply to their next 25-second announce. Chunks are stored under their SHA-256, so versions never overwrite each other.

## Implementation

`make_manifest.py` splits the source into hash-named chunks and creates its versioned manifest. `tracker.py` maintains the availability index with a 90-second lease, stores the latest version of each file, and pushes update notifications. `peer.py` exposes local chunks, fetches missing chunks, validates them, announces them, reconstructs the file, and follows updates; on the seeder, `--watch` publishes a new version whenever the source file is saved. The implementation uses only Python’s standard library, making it easy to run in a lab or Mininet namespace (replace the loopback endpoints with Mininet host IPs).

## Test cases

| Case | Method | Expected result |
|---|---|---|
| Normal transfer | one seeder, one downloader | reconstructed digest matches source |
| Multi-peer seeding | run two downloaders concurrently | logs show chunks served by peers that are still downloading |
| Peer failure | stop initial seeder mid-transfer after another peer has chunks | client retries and completes from remaining peers |
| Corruption | modify a chunk before it is served | receiver reports failed SHA-256 and does not save it |
| Resume | stop and restart downloader with same chunk folder | already valid chunks are reused |
| File update | edit and save the source while peers run | every peer prints “Update available”, downloads only changed chunks, and its output digest matches the new source |
| Missed update | start a peer with the old manifest after an update | it detects the newer version on announce and builds the latest one |

## Metrics and results

Record the final rate printed by the peer and calculate `throughput = file_size / completion_time`. For each run capture: chunk size, peers, source size, completion time, throughput, number of failed attempts, and successful uploads per peer. Repeat with 1, 2, and 3 seeders and plot throughput against number of peers. Expected observation: more sources improve availability and may improve throughput, while tiny chunks increase protocol overhead.

For updates, also record the time from saving the file to every peer printing “Complete … v2”, and the number of chunks re-downloaded out of the total. In a local test with a 640 KB file (10 chunks), editing 100 bytes inside one chunk made each peer re-download 1 of 10 chunks, and all rebuilt files matched the new source's SHA-256.

## Five-minute live demonstration

1. Start the tracker and split a small file (20 seconds).
2. Start the initial seeder with `--watch` and show its chunk count (20 seconds).
3. Start peers A and B; point out “Downloaded and seeded” lines (60 seconds).
4. Stop the original seeder after pieces spread; show download completes using A/B (60 seconds).
5. Compare SHA-256 values and show the tracker/peer message fields (40 seconds).
6. Restart the seeder, then edit and save the source file; show the “Update available” lines and how few chunks were re-downloaded, then compare SHA-256 values again (60 seconds).
7. Present the throughput line, architecture, and each member’s component (40 seconds).
