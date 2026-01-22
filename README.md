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

### Prerequisites

```bash
pip install flask requests watchdog
```

### Single Node Setup

```bash
# Terminal 1: Start blockchain service
export NODE_ID=node-1
export NODE_IP=127.0.0.1
export CLUSTER_IPS=127.0.0.1
python3 blockchain-service.py

# Terminal 2: Start watchdog
export BLOCKCHAIN_API=http://127.0.0.1:5000/event
export WATCH_PATH=./watched-directory
python3 watchdog-images.py
```

### 3-Node Cluster Setup

See [API_DOCUMENTATION.md](API_DOCUMENTATION.md#complete-setup-3-node-cluster) for complete multi-node setup.

## Documentation

- **[API Documentation](API_DOCUMENTATION.md)** - Complete API reference, configuration, and usage guide
- **[Solution Summary](SOLUTION_SUMMARY.md)** - High-level overview of fixes and improvements
- **[Issues Analysis](ISSUES_ANALYSIS.md)** - Detailed analysis of 28 identified issues and fixes

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

## Testing

```bash
# Run test suite
python3 test_blockchain.py

# Manual testing
touch /path/to/watched/file.qcow2
curl http://localhost:5000/get_blocks
curl http://localhost:5000/status
```

## Performance

- **Block Lookup**: O(1) via hash index
- **WAL Append**: O(1) append-only writes
- **Chain Size**: Scales to millions of blocks
- **Cluster Size**: Tested with 3-5 nodes
- **Event Rate**: Limited by consensus latency (~3s per block)

## Security

⚠️ **Current Implementation**: No authentication, no TLS, weak signatures

**For Production**: Add authentication, use HTTPS, implement cryptographic signatures, add rate limiting.

See [API_DOCUMENTATION.md - Security](API_DOCUMENTATION.md#security-considerations) for details.

## Troubleshooting

### Blocks Not Committing

Check node health, verify network connectivity, inspect logs for replication failures.

### Chain Divergence

Compare chains across nodes, check for term bumps, restart nodes to force sync.

### High Retry Queue

Verify cluster quorum is possible, check all nodes are healthy, restart failed nodes.

See [API_DOCUMENTATION.md - Troubleshooting](API_DOCUMENTATION.md#troubleshooting) for complete guide.

## Contributing

This project was developed to solve multi-node consistency issues in distributed filesystem event tracking. For issues identified and fixes applied, see [ISSUES_ANALYSIS.md](ISSUES_ANALYSIS.md).

## License

See LICENSE file for details.
