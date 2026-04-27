# API Documentation

The Stasis Blockchain node runs a Flask API on port 5000 (configurable via API_PORT discovery broadcast).

## Event Submission

### `POST /event`

Receives a filesystem event from the watchdog.

**Request Body**:

```JSON
{
  "event": "create|modify|delete|move",
  "path": "/path/to/file.qcow2",
  "inode": 12345,
  "size_bytes": 1048576,
  "content_hash": "sampled:abc123...",
  "metadata": {},
  "dest_path": "/optional/dest/path"
}
```

**Responses:**

- `200 OK`: `{"status": "committed"}` - Block successfully added via consensus.

- `200 OK`: `{"status": "queued"}` - Consensus failed, event queued for local background retry.

- `200 OK`: `{"status": "redirect", "leader": "<leader_ip>"}` - Sent to a Follower; client should redirect payload to the Leader.

## Internal Raft & Consensus Endpoints

### `POST /raft/vote`

Handles a Raft vote request from a candidate.

- Request: `{"term": 1, "candidate_id": "node-2", "candidate_ip": "172.16.22.3"}`

- Response: `{"vote_granted": true|false, "term": 1}`

### `POST /raft/heartbeat`

Receives a heartbeat from the current Raft leader to maintain authority.

- Request: `{"term": 1, "leader_ip": "172.16.22.2", "leader_id": "node-1"}`

- Response: `{"accepted": true|false}`

### `POST /replicate`

Replicate a block from the Leader to Followers.

- Request: Complete block JSON object.

- Responses:
  - `200 OK`: `{"status": "ok"}` (Block applied) or `{"status": "exists"}` (Already present).
  - `400 Bad Request`: `{"error": "invalid", "reason": "chain validation failed"}`.
  - `409 Conflict`: `{"error": "corrupt_hash", "block_index": 5}` (Computed hash mismatch).

## Synchronization & State

### `GET /get_blocks`

Return blocks in the blockchain, optionally filtered by index.

- Query Parameters: `?since=<index>`

- Response: JSON array of block objects where index > since.

### `GET /sync_full`

Returns the complete blockchain for a bootstrapping node.

- Response: Complete JSON array of all block objects.

### `GET /health`

Returns local node health, Raft role, and chain length.

- Response: `{"status": "healthy", "node_id": "node-1", "term": 2, "role": "leader", "leader_ip": "...", "chain_length": 15}`

### `GET /cluster_status`

Queries known peers and aggregates cluster-wide node status.

- Response: JSON detailing nodes array, total count, healthy count, and leader_ip.

### `GET /status`

Returns an HTML dashboard mapping the distributed blockchain status, visualizing node readiness, chain indices, and block payload data.
