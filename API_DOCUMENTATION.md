# Blockchain Volume Ledger - API Documentation

## Overview

This system provides a distributed blockchain for tracking filesystem events across multiple nodes. It consists of two main components:

1. **blockchain-service.py** - The blockchain node service
2. **watchdog-images.py** - Filesystem event monitor

## Architecture

```
┌─────────────────┐
│ Distributed FS  │
│   (.qcow2 files)│
└────────┬────────┘
         │ monitors
         ▼
┌─────────────────┐
│ Watchdog        │
│ (watches files) │
└────────┬────────┘
         │ HTTP POST /event
         ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│ Blockchain Node │◄────►│ Blockchain Node │◄────►│ Blockchain Node │
│  (Leader)       │      │  (Follower)     │      │  (Follower)     │
└─────────────────┘      └─────────────────┘      └─────────────────┘
         │
         │ /replicate (consensus)
         ▼
  ┌─────────────┐
  │     WAL     │ (Write-Ahead Log - append-only)
  └─────────────┘
```

## Blockchain Service API

### Base URL
`http://<node-ip>:5000`

---

### POST /event

Receive a filesystem event from watchdog for processing.

**Request Body:**
```json
{
  "event": "create|modify|delete|move",
  "path": "/path/to/file.qcow2",
  "inode": 12345,
  "size_bytes": 1024000,
  "content_hash": "abc123...",
  "metadata": {},
  "dest_path": "/path/to/dest.qcow2"  // only for move events
}
```

**Response:**
```json
// Success - block committed
{
  "status": "committed"
}

// Queued - consensus failed, will retry
{
  "status": "queued"
}

// Not leader - redirect to leader
{
  "status": "ignored",
  "leader": "192.168.1.10"
}
```

**Status Codes:**
- `200` - Event processed (check status field)

**Algorithm:**
1. Compute event affinity hash from inode + content_hash + event
2. Select leader based on affinity and node health
3. If not leader, return leader IP for client retry
4. If leader, create block and run consensus
5. Return committed/queued status

---

### POST /replicate

Internal endpoint for block replication between nodes (consensus protocol).

**Request Body:**
```json
{
  "term": 5,
  "block_version": 1,
  "timestamp": 1642857600.0,
  "index": 42,
  "node_id": "node-1",
  "event_id": "sha256...",
  "event": "create",
  "path": "/images/vm001.qcow2",
  "inode": 12345,
  "size_bytes": 1024000,
  "content_hash": "abc123...",
  "prev_block_hash": "def456...",
  "metadata": {},
  "signature": "node_key...",
  "block_hash": "ghi789..."
}
```

**Response:**
```json
// Block accepted
{
  "status": "ok"
}

// Block already exists
{
  "status": "exists"
}

// Validation failed
{
  "error": "invalid"
}
```

**Status Codes:**
- `200` - Block accepted or exists
- `400` - Block validation failed

**Validation Rules:**
- Block index must be `last_index + 1`
- `prev_block_hash` must match current chain tip
- `block_hash` must be correctly computed

---

### GET /get_blocks

Retrieve all blocks in the blockchain.

**Response:**
```json
[
  {
    "term": 5,
    "block_version": 1,
    "timestamp": 1642857600.0,
    "index": 1,
    "node_id": "node-1",
    "event_id": "sha256...",
    "event": "create",
    "path": "/images/vm001.qcow2",
    "inode": 12345,
    "size_bytes": 1024000,
    "content_hash": "abc123...",
    "prev_block_hash": "0000...",
    "metadata": {},
    "signature": "node_key...",
    "block_hash": "abc123..."
  },
  ...
]
```

**Status Codes:**
- `200` - Success

**Usage:**
- Used by sync process to fetch blockchain from other nodes
- Called periodically by `sync_blockchain()` background thread

---

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "term": 5
}
```

**Status Codes:**
- `200` - Node is healthy

**Usage:**
- Used by leader selection to check node availability
- Used by monitoring systems
- Returns current term for tie-breaking in leader election

---

### GET /status

Web UI showing blockchain status across all nodes.

**Response:**
HTML page displaying:
- Node ID and cluster configuration
- All blocks with metadata
- Which nodes have each block
- Chain statistics

**Status Codes:**
- `200` - HTML page

**Usage:**
- Human-readable blockchain explorer
- Debugging and monitoring

---

## Configuration

### Environment Variables

#### Blockchain Service (blockchain-service.py)

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node-1` | Unique identifier for this node |
| `NODE_IP` | `127.0.0.1` | IP address of this node |
| `CLUSTER_IPS` | `` | Comma-separated list of all cluster node IPs |
| `DATA_DIR` | `./data` | Directory for persistent storage (WAL, term, seen events) |
| `SEEN_EVENTS_PERSIST_INTERVAL` | `10` | Persist seen events every N blocks |

**Example:**
```bash
export NODE_ID=node-1
export NODE_IP=192.168.1.10
export CLUSTER_IPS=192.168.1.10,192.168.1.11,192.168.1.12
export DATA_DIR=/var/lib/blockchain
export SEEN_EVENTS_PERSIST_INTERVAL=20
python3 blockchain-service.py
```

#### Watchdog Service (watchdog-images.py)

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOCKCHAIN_API` | `http://blockchain:5000/event` | URL of blockchain service |
| `WATCH_PATH` | `/images` | Directory to monitor for file changes |

**Example:**
```bash
export BLOCKCHAIN_API=http://192.168.1.10:5000/event
export WATCH_PATH=/mnt/distributed-fs
python3 watchdog-images.py
```

---

## Leader Election Algorithm

### Custom Affinity-Based Leader Selection

The system uses a custom leader election algorithm that balances deterministic event routing with dynamic health-based failover:

#### Algorithm Steps:

1. **Event Affinity Hash**
   ```python
   key = f"{inode}:{content_hash}:{event}"
   affinity_hash = sha256(key)
   start_idx = affinity_hash % len(CLUSTER_IPS)
   ```
   - Same event always starts with same candidate
   - Reduces conflicts for related events

2. **Health Check**
   ```python
   for each node starting from start_idx:
       query /health endpoint
       collect (ip, term, is_healthy)
   ```
   - Checks if nodes are alive and responsive
   - Retrieves current term for each node

3. **Prioritization**
   ```python
   sort by:
       1. is_healthy (alive nodes first)
       2. term (higher term first)
       3. original cluster order
   ```
   - Healthy nodes are preferred
   - Higher term nodes break ties (more up-to-date)
   - Deterministic fallback based on cluster order

4. **Selection**
   - Select first node from sorted list
   - If no nodes available, use self as leader

#### Advantages:
- **Event Affinity**: Same events go to same leader (reduces conflicts)
- **Automatic Failover**: Unhealthy nodes are skipped
- **Term-Based**: Higher-term nodes are preferred (better state)
- **Deterministic**: Given same inputs, produces same result
- **Load Balancing**: Events distributed across cluster

#### Improvements Over Simple Hash:
- ✅ Considers node health (not just hash)
- ✅ Uses term for conflict resolution
- ✅ Automatic failover when leader fails
- ✅ No need for separate leader election rounds
- ✅ Works well for event-driven workloads

---

## Consensus Protocol

### Quorum-Based Consensus

The system uses quorum consensus for block commits:

```python
quorum_size = (num_nodes // 2) + 1

# Example with 3 nodes:
quorum = (3 // 2) + 1 = 2 nodes

# Example with 5 nodes:
quorum = (5 // 2) + 1 = 3 nodes
```

#### Consensus Flow:

1. **Leader Creates Block**
   ```
   Leader: create_block(event)
   ```

2. **Replicate to Followers**
   ```
   For each follower:
       POST /replicate with block
       Wait for acknowledgment
   ```

3. **Count Acknowledgments**
   ```
   acks = 1 (self)
   for each successful replication:
       acks += 1
   ```

4. **Check Quorum**
   ```
   if acks >= quorum_size:
       apply_block()
       return success
   else:
       bump_term()
       queue_for_retry()
       return failure
   ```

#### Failure Handling:

- **Quorum Not Reached**: Increment term, queue event for retry
- **Replication Timeout**: Count as failure, continue with other nodes
- **Invalid Block**: Reject, return error to sender
- **Duplicate Block**: Accept silently (idempotent)

---

## Data Persistence

### Write-Ahead Log (WAL)

#### Format:
- **Type**: Append-only line-delimited JSON
- **Location**: `{DATA_DIR}/blockchain.wal`
- **Format**: One block per line

```
{"index":1,"event":"create",...}\n
{"index":2,"event":"modify",...}\n
{"index":3,"event":"delete",...}\n
```

#### Characteristics:
- ✅ **Append-only**: No rewrites, efficient for large chains
- ✅ **Durable**: Uses fsync before returning
- ✅ **WAL-first**: Written before in-memory update
- ✅ **Crash-safe**: Survives node restarts

#### Recovery:
```python
On startup:
    if WAL exists:
        for each line in WAL:
            parse block
            append to chain
            rebuild hash index
```

### Term Storage

- **File**: `{DATA_DIR}/term.json`
- **Format**: `{"term": 5}`
- **Durability**: Atomic write (temp file + fsync + rename)

### Seen Events

- **File**: `{DATA_DIR}/seen_events.json`
- **Format**: `{"events": ["hash1", "hash2", ...]}`
- **Persistence**: Every N blocks (configurable)
- **Purpose**: Prevent duplicate processing after restart

---

## Thread Safety

### Locking Strategy

The system uses **granular locking** for different operations:

| Lock | Protects | Held During |
|------|----------|-------------|
| `append_lock` | Blockchain chain | Block creation, consensus, application |
| `term_lock` | Term operations | Term load/persist/bump |
| `retry_lock` | Retry queue | Queue append/remove |
| `seen_lock` | Seen events set | Duplicate checking, persist |

### Double-Check Locking Pattern

```python
# Quick check without lock (fast path)
with seen_lock:
    if event_id in seen_events:
        return None

# Slow path - acquire exclusive lock
with append_lock:
    # Double-check after acquiring lock
    with seen_lock:
        if event_id in seen_events:
            return None
    
    # Guaranteed unique at this point
    create_and_commit_block()
```

**Purpose**: Prevent race conditions where two threads process same event

---

## Error Handling

### Consensus Failures

**Cause**: Quorum not reached
**Action**: 
1. Increment term
2. Add event to retry queue
3. Log failure
4. Background thread retries later

### Network Failures

**Cause**: Node unreachable during replication
**Action**:
1. Log error with details
2. Continue with other nodes
3. Check if quorum still reachable
4. Sync process will catch up node when it recovers

### Validation Failures

**Cause**: Invalid block (bad hash, wrong index, broken chain)
**Action**:
1. Reject block
2. Return 400 error
3. Log validation failure
4. Do not apply block

### Sync Gaps

**Cause**: Missing blocks in chain (e.g., have block 5, receive block 7)
**Action**:
1. Log gap detection
2. Stop processing blocks from that node
3. Try next node
4. Background sync will eventually fill gap

---

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Block lookup | O(1) | Via hash index |
| Block validation | O(1) | Hash computation |
| WAL append | O(1) | Single line append + fsync |
| Consensus | O(N) | N = cluster size |
| Sync | O(M log M) | M = missing blocks (sorting required) |
| Event deduplication | O(1) | Hash set lookup |

### Scalability

- **Chain Size**: Efficient up to millions of blocks (append-only WAL)
- **Cluster Size**: Tested with 3-5 nodes, should work with more
- **Event Rate**: Limited by consensus latency (~3s timeout per block)
- **Storage**: Linear growth (one JSON line per block)

---

## Monitoring

### Logs

All components log to stderr with format:
```
[2024-01-22 15:30:45,123] INFO: Message here
```

**Log Levels:**
- `DEBUG`: Detailed internal operations
- `INFO`: Normal operations (block created, synced)
- `WARNING`: Recoverable errors (consensus failure, sync gap)
- `ERROR`: Serious errors (WAL write failure)

### Metrics to Monitor

1. **Block Rate**: Blocks added per second
2. **Consensus Success Rate**: % of blocks reaching quorum
3. **Sync Gaps**: Number of gaps detected
4. **Term Bumps**: Frequency of term increments (indicates failures)
5. **Retry Queue Size**: Number of pending retries
6. **Chain Length**: Total blocks in blockchain

### Health Checks

```bash
# Check node health
curl http://node:5000/health

# Get blockchain
curl http://node:5000/get_blocks

# View status page
open http://node:5000/status
```

---

## Security Considerations

### Current Implementation

⚠️ **No authentication/authorization** - Endpoints are open
⚠️ **No TLS** - Inter-node communication is plaintext
⚠️ **Weak signatures** - node_key is random, not cryptographic signature

### Recommendations for Production

1. **Add API Authentication**
   ```python
   @app.before_request
   def authenticate():
       token = request.headers.get('Authorization')
       if not validate_token(token):
           abort(401)
   ```

2. **Use TLS**
   ```bash
   # Use HTTPS for all inter-node communication
   requests.post('https://node:5000/replicate', ...)
   ```

3. **Implement Real Signatures**
   ```python
   import ed25519
   signature = private_key.sign(block_json)
   ```

4. **Rate Limiting**
   ```python
   from flask_limiter import Limiter
   limiter = Limiter(app, key_func=get_remote_address)
   
   @app.route('/event')
   @limiter.limit("10/minute")
   def receive_event():
       ...
   ```

---

## Troubleshooting

### Problem: Blocks Not Committing

**Symptoms**: Events return "queued", retry queue grows

**Possible Causes:**
1. Network issues between nodes
2. Nodes unreachable (check `/health`)
3. Clock skew (validate timestamps)

**Solutions:**
```bash
# Check node health
curl http://node1:5000/health
curl http://node2:5000/health

# Check logs for replication failures
grep "Replication failed" logs

# Verify network connectivity
ping node2
telnet node2 5000
```

### Problem: Chain Divergence

**Symptoms**: Different nodes have different blocks at same index

**Possible Causes:**
1. Network partition during consensus
2. Bugs in validation logic
3. Manual data corruption

**Solutions:**
```bash
# Compare chains
diff <(curl node1:5000/get_blocks) <(curl node2:5000/get_blocks)

# Check for term bumps (indicates failures)
grep "Bumped term" logs

# Force sync (restart node)
```

### Problem: High Retry Queue

**Symptoms**: Many events stuck in retry queue

**Possible Causes:**
1. Persistent consensus failures
2. Cluster too small (no quorum possible)
3. All nodes down except one

**Solutions:**
```bash
# Check cluster status
for node in node1 node2 node3; do
    echo "$node: $(curl -s http://$node:5000/health)"
done

# Restart failed nodes
systemctl restart blockchain-service

# Check retry queue size
curl http://node:5000/status | grep -i retry
```

---

## Examples

### Complete Setup: 3-Node Cluster

**Node 1:**
```bash
export NODE_ID=node-1
export NODE_IP=192.168.1.10
export CLUSTER_IPS=192.168.1.10,192.168.1.11,192.168.1.12
export DATA_DIR=/var/lib/blockchain
python3 blockchain-service.py &

export BLOCKCHAIN_API=http://192.168.1.10:5000/event
export WATCH_PATH=/mnt/shared-storage
python3 watchdog-images.py &
```

**Node 2:**
```bash
export NODE_ID=node-2
export NODE_IP=192.168.1.11
export CLUSTER_IPS=192.168.1.10,192.168.1.11,192.168.1.12
export DATA_DIR=/var/lib/blockchain
python3 blockchain-service.py &

export BLOCKCHAIN_API=http://192.168.1.11:5000/event
export WATCH_PATH=/mnt/shared-storage
python3 watchdog-images.py &
```

**Node 3:**
```bash
export NODE_ID=node-3
export NODE_IP=192.168.1.12
export CLUSTER_IPS=192.168.1.10,192.168.1.11,192.168.1.12
export DATA_DIR=/var/lib/blockchain
python3 blockchain-service.py &

export BLOCKCHAIN_API=http://192.168.1.12:5000/event
export WATCH_PATH=/mnt/shared-storage
python3 watchdog-images.py &
```

### Testing

```bash
# Create a file to trigger event
touch /mnt/shared-storage/test.qcow2

# Check all nodes have the block
curl http://192.168.1.10:5000/get_blocks | jq 'length'
curl http://192.168.1.11:5000/get_blocks | jq 'length'
curl http://192.168.1.12:5000/get_blocks | jq 'length'

# Should all return the same count
```

---

## Glossary

- **Block**: Immutable record of a filesystem event
- **Chain**: Ordered sequence of blocks
- **Consensus**: Agreement protocol between nodes
- **Event**: Filesystem change (create, modify, delete, move)
- **Leader**: Node responsible for proposing new blocks
- **Quorum**: Minimum number of nodes needed for decision
- **Term**: Epoch number for conflict resolution
- **WAL**: Write-Ahead Log - persistent append-only storage
- **Event Affinity**: Mapping events to consistent leaders via hash
- **Hash Chain**: Each block references previous block's hash

---

## References

- **Source Code**: `blockchain-service.py`, `watchdog-images.py`
- **Issue Analysis**: `ISSUES_ANALYSIS.md`
- **Solution Summary**: `SOLUTION_SUMMARY.md`
- **Tests**: `test_blockchain.py`
