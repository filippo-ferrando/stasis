# Cluster Recovery & Resilience Mechanisms

The Stasis blockchain handles distributed state anomalies automatically through background threads and its consensus architecture.

1. **New Host Joins the Cluster**
    - Discovery: When a new node boots, its `UDPDiscovery` service begins broadcasting beacons. Existing nodes receive the beacon and add the new node's IP to their active peers list (`_peers`).

    - Bootstrapping: The background `sync_loop()` on the new node recognizes its `last_index` is `0`. It executes `_bootstrap_from_peers()`.

    - Sync Process: It polls the `/health` endpoint of all discovered peers to find the node reporting the longest chain_length. It then calls `/sync_full` on that peer, downloading and locally validating/applying the entire block history sequentially.

2. **Leader Leaves or Crashes**

    - Heartbeat Timeout: The Leader continuously broadcasts heartbeats (`/raft/heartbeat`). If a Follower does not receive a heartbeat within its randomized `_election_timeout` (between *4.0* and *8.0* seconds), it assumes the **Leader** is dead.

    - Election Triggered: The Follower bumps its local term (`current_term += 1`), transitions to `CANDIDATE` role, votes for itself, and blasts `/raft/vote` to all discovered peers.

    - Resolution: The first `CANDIDATE` to receive a vote majority (`quorum = (len(peers) + 1) // 2 + 1`) transitions to `LEADER` and begins sending its own heartbeats.

3. **Host Receives a Divergent / Future Block**

    - Validation Rejection: If the Leader attempts to push a block via `POST /replicate` and the receiving Follower detects a gap (`block["index"] != last_index + 1`) or a hash mismatch (`prev_block_hash != last_hash`), the Follower immediately rejects it with a `400 Bad Request`.

    - Background Catch-up: The Follower's background `sync_loop()` continuously runs every 5 seconds. It contacts peers via `/get_blocks?since=<self.last_index>`.

    - Sequential Application: The Follower downloads missing blocks, sorts them by index, and applies them strictly sequentially. Once caught up, it will successfully accept future replicated blocks.

4. **Failed Consensus (Quorum Not Reached)**

    - If the Leader creates a block but fails to receive `200 OK` from a majority of nodes during `POST /replicate`, the block is not committed.

    - The Leader triggers `bump_term()` to advance the epoch (invalidating the broken round) and appends the raw event payload to its `retry_queue`.

    - A background `retry_loop()` continually attempts to re-propose events in the queue until a cluster quorum is restored.

5. **Local Crash and Restart**

    - WAL Recovery: Before processing new requests, the node executes `load_wal()`. It reads blockchain.wal line by line, reconstructing the exact in-memory chain, `last_index`, and `block_hash_index`.

    - Term & Seen Events Recovery: It loads its persistent Raft term from `term.json` and its deduplication set from `seen_events.json` to prevent re-processing events it already broadcasted.
