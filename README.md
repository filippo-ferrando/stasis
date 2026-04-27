<p align="center">
 <img src="logo.png" alt="Stasis Logo" width="600"/>
</p>

# Stasis: Distributed Blockchain Volume Ledger

Stasis is a distributed blockchain system designed to track filesystem events across multiple nodes, ensuring multi-node consistency, crash recovery, and deduplication.

## Architecture

The system consists of three main operational components:

1. Blockchain API Node (`stasis-api.py`): A Flask-based web service that implements a Raft-inspired consensus algorithm, a Write-Ahead Log (WAL), and maintains the blockchain.

2. Watchdog (`stasis-watchdog.py`): A filesystem observer that monitors `.qcow2` files for changes (create, modify, delete, move) and submits events to the Blockchain API.

3. UDP Discovery (`stasis_discovery.py`): A decentralized peer discovery mechanism using UDP broadcasts to dynamically track cluster members.

## Core Mechanisms

1. Consensus: Nodes participate in a Raft-inspired leader election process. The elected Leader is responsible for receiving events and replicating blocks to Followers.

2. Dynamic Peer Discovery: Nodes do not require hardcoded peer lists. They broadcast UDP beacons (default port `7000`) every `DISCOVERY_INTERVAL` seconds.

3. Write-Ahead Log (WAL): Blocks are persisted locally to a file (`blockchain.wal`) in an append-only fashion before memory structures are updated.

4. Smart Hashing: Large `.qcow2` files use a sampled **BLAKE3** fingerprinting mechanism. Files exceeding `HASH_FULL_THRESHOLD_MB` are hashed by reading evenly spaced chunks, bounding the hashing time while detecting mutations.

## Environment Variables

### Blockchain Service (stasis-api.py)

- `NODE_ID`: Unique node identifier (default: `node-1`)
- `NODE_IP`: The IP address of the node (default: `127.0.0.1`)
- `DATA_DIR`: Directory for WAL, term, and seen events (default: `./data`)
- `SEEN_EVENTS_PERSIST_INTERVAL`: How often to persist deduplication data (default: `10`)
- `DISCOVERY_PORT`: UDP port for discovery beacons (default: `7000`)
- `DISCOVERY_INTERVAL`: Seconds between beacons (default: `5`)
- `PEER_TTL`: Seconds before a silent peer is removed (default: `15`)
- `DISCOVERY_BROADCAST`: Broadcast address (default: `255.255.255.255`)
- `API_PORT`: API port advertised in the beacon (default: `5000`)

### Watchdog Service (stasis-watchdog.py)

- `BLOCKCHAIN_API`: URL of the blockchain service (default: `http://blockchain:5000/event`)
- `WATCH_PATH`: Directory to monitor for .qcow2 files (default: `/images`)
- `HASH_FULL_THRESHOLD_MB`: Threshold for full vs. sampled hashing (default: `512`)
- `HASH_SAMPLE_COUNT`: Number of chunks for sampled hashing (default: `64`)
- `HASH_SAMPLE_SIZE_MB`: Chunk size in MB (default: `4`)
