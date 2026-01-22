# Distributed Blockchain Volume Ledger

A distributed blockchain system for tracking filesystem events across multiple nodes with multi-node consistency, crash recovery, and event deduplication.

## Overview

This system provides an immutable, distributed ledger for recording filesystem changes (create, modify, delete, move) on distributed filesystems. It uses a custom blockchain implementation with quorum consensus to ensure consistency across nodes.

### Key Features

- **Multi-Node Consistency**: Quorum-based consensus prevents blockchain divergence
- **Crash Recovery**: Write-Ahead Log (WAL) with fsync ensures durability
- **Event Deduplication**: Prevents duplicate processing via persistent seen events
- **Custom Leader Election**: Affinity-based selection with health checks and term prioritization
- **Thread-Safe**: Granular locking strategy prevents race conditions
- **Append-Only WAL**: Efficient storage that scales to millions of blocks
- **Real-Time Monitoring**: Watch filesystem changes and record immediately

### Architecture

```
Distributed FS → Watchdog Monitor → Blockchain Nodes (Consensus) → WAL Storage
```

## Quick Start

### `docker-compose` deploy

In the `docker` folder you will find a compose file that will create a test environment to try out the blockchain.
See the blockchain status on `http://localhost:5001/status` (page served by node-1)

Docker image isn't pushed to registry -> manual buiding is required, use the `build.sh` script in the root of this repo to build the images from scratch (also run `run.sh` to launch the compose file)

## Documentation

- **[API Documentation](API_DOCUMENTATION.md)** - Complete API reference, configuration, and usage guide

## Components

### blockchain-service.py

The core blockchain node service that:
- Maintains the blockchain of filesystem events
- Implements quorum consensus for multi-node agreement
- Provides HTTP API for event submission and replication
- Manages Write-Ahead Log for crash recovery
- Performs leader selection and node synchronization

### watchdog-images.py

Filesystem monitor that:
- Watches a directory for .qcow2 file changes
- Computes SHA256 hashes of file contents
- Sends events to blockchain service via HTTP
- Handles create, modify, delete, and move events

## Configuration

### Blockchain Service

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node-1` | Unique identifier for this node |
| `NODE_IP` | `127.0.0.1` | IP address of this node |
| `CLUSTER_IPS` | `` | Comma-separated list of all cluster node IPs |
| `DATA_DIR` | `./data` | Directory for persistent storage |
| `SEEN_EVENTS_PERSIST_INTERVAL` | `10` | Persist seen events every N blocks |

### Watchdog Service

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOCKCHAIN_API` | `http://blockchain:5000/event` | URL of blockchain service |
| `WATCH_PATH` | `/images` | Directory to monitor |

## API Endpoints

- `POST /event` - Submit filesystem event
- `POST /replicate` - Replicate block (internal consensus)
- `GET /get_blocks` - Retrieve all blocks
- `GET /health` - Health check
- `GET /status` - Web UI for blockchain status

See [API_DOCUMENTATION.md](API_DOCUMENTATION.md) for complete API reference.

## How It Works

### Event Flow

1. **Detection**: Watchdog detects file change
2. **Hash**: Computes SHA256 of file content
3. **Submit**: Sends event to blockchain service
4. **Leader Selection**: Service selects leader based on event affinity
5. **Consensus**: Leader replicates to cluster, waits for quorum
6. **Commit**: Block written to WAL and applied to chain
7. **Sync**: Other nodes sync periodically

### Leader Election

Uses custom affinity-based selection:
- Events with same inode/hash always route to same leader candidate
- Health checks determine which nodes are available
- Term numbers break ties (higher term preferred)
- Automatic failover if leader is down

### Consensus Protocol

Quorum-based (majority):
- Leader creates block
- Replicates to all followers
- Waits for majority acknowledgment
- Commits if quorum reached
- Bumps term and retries if failed

## Performance

- **Block Lookup**: O(1) via hash index
- **WAL Append**: O(1) append-only writes
- **Chain Size**: Scales to millions of blocks
- **Cluster Size**: Tested with 3-5 nodes
- **Event Rate**: Limited by consensus latency (~3s per block)

## Security

⚠️ **Current Implementation**: No authentication, no TLS, weak signatures

**For Production**: Add authentication, use HTTPS, implement cryptographic signatures, add rate limiting.

## Troubleshooting

### Blocks Not Committing

Check node health, verify network connectivity, inspect logs for replication failures.

### Chain Divergence

Compare chains across nodes, check for term bumps, restart nodes to force sync.

### High Retry Queue

Verify cluster quorum is possible, check all nodes are healthy, restart failed 

## Todo
 - implement security measures
 - change hashing function (qcow2 images can be HUGE)
 - watchdog need rework (creating a file generates 2 event)
