#!/usr/bin/env python3
import os
import json
import time
import hashlib
import threading
import logging
import requests
from flask import Flask, request, jsonify, render_template

# ======================
# Environment
# ======================
NODE_ID = os.environ.get("NODE_ID", "node-1")
NODE_IP = os.environ.get("NODE_IP", "127.0.0.1")
CLUSTER_IPS = [
    ip.strip() for ip in os.environ.get("CLUSTER_IPS", "").split(",") if ip.strip()
]

DATA_DIR = os.environ.get("DATA_DIR", "./data")
WAL_PATH = os.path.join(DATA_DIR, "blockchain.wal")
TERM_PATH = os.path.join(DATA_DIR, "term.json")
SEEN_EVENTS_PATH = os.path.join(DATA_DIR, "seen_events.json")

# Configuration
SEEN_EVENTS_PERSIST_INTERVAL = int(os.environ.get("SEEN_EVENTS_PERSIST_INTERVAL", "10"))

os.makedirs(DATA_DIR, exist_ok=True)

# ======================
# Flask
# ======================
app = Flask(__name__)


# ======================
# Blockchain
# ======================
class Blockchain:
    """
    Distributed blockchain for tracking filesystem events with multi-node consistency.

    This class implements a blockchain that:
    - Records filesystem events (create, modify, delete, move) as blocks
    - Maintains consistency across multiple nodes using quorum consensus
    - Provides crash recovery via Write-Ahead Log (WAL)
    - Prevents duplicate event processing via persistent deduplication
    - Uses custom leader selection for load balancing

    Architecture:
    - WAL-first persistence: Changes written to disk before memory updates
    - Granular locking: Separate locks for chain, term, retry queue, and seen events
    - O(1) block lookups via hash index
    - Append-only WAL for efficient writes

    Thread Safety:
    - append_lock: Protects blockchain chain modifications
    - term_lock: Protects term/epoch operations
    - retry_lock: Protects retry queue access
    - seen_lock: Protects seen_events set access

    Attributes:
        chain (list): The blockchain - list of blocks
        last_index (int): Index of the last block in chain
        last_hash (str): Hash of the last block
        current_term (int): Current term/epoch number for conflict resolution
        node_key (str): Unique identifier for this node
        retry_queue (list): Queue of events that failed consensus
        seen_events (set): Set of event IDs already processed
        block_hash_index (dict): Hash index for O(1) block lookups
    """

    def __init__(self):
        """
        Initialize the blockchain node.

        Sets up logging, initializes data structures, creates locks for thread safety,
        and loads persistent state from disk (term, WAL, seen events).
        """
        self.logger = logging.getLogger("blockchain")
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        )
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

        self.chain = []
        self.last_index = 0
        self.last_hash = "0" * 64

        self.current_term = 0
        self.node_key = hashlib.sha256(os.urandom(32)).hexdigest()

        # Enhanced locking strategy for thread safety
        self.append_lock = threading.Lock()  # Protects chain modifications
        self.term_lock = threading.Lock()  # Protects term operations
        self.retry_lock = threading.Lock()  # Protects retry queue
        self.seen_lock = threading.Lock()  # Protects seen_events

        self.retry_queue = []
        self.seen_events = set()
        self.block_hash_index = {}  # Fast O(1) lookup for has_block()

        self.load_term()
        self.load_wal()
        self.load_seen_events()

    # ======================
    # Term / Epoch
    # ======================
    def load_term(self):
        """
        Load the current term number from persistent storage.

        The term is used for conflict resolution - higher terms take precedence.
        If no term file exists, initializes with term 0.

        Thread-safe via term_lock.
        """
        with self.term_lock:
            if os.path.exists(TERM_PATH):
                with open(TERM_PATH) as f:
                    self.current_term = json.load(f)["term"]
            else:
                self.persist_term()

    def persist_term(self):
        """
        Persist the current term to disk atomically.

        Uses temp file + fsync + atomic rename to ensure durability.
        This prevents term corruption on crash.
        """
        # Write to temp file first, then atomic rename for durability
        temp_path = TERM_PATH + ".tmp"
        with open(temp_path, "w") as f:
            json.dump({"term": self.current_term}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, TERM_PATH)

    def bump_term(self):
        """
        Increment the term number and persist it.

        Called when consensus fails to reach quorum. The higher term helps
        resolve conflicts when the node participates in future consensus rounds.

        Thread-safe via term_lock.
        """
        with self.term_lock:
            self.current_term += 1
            self.persist_term()
            self.logger.warning(f"Bumped term to {self.current_term}")

    # ======================
    # WAL
    # ======================
    def load_wal(self):
        """
        Load the blockchain from the Write-Ahead Log (WAL).

        Reads all blocks from the WAL file and reconstructs the in-memory
        blockchain state. Also rebuilds the block hash index and seen events set.

        Called during initialization to recover state after restart.
        """
        if not os.path.exists(WAL_PATH):
            self.logger.info("No WAL found, starting fresh.")
            return
        with open(WAL_PATH) as f:
            for line in f:
                block = json.loads(line)
                self.chain.append(block)
                self.last_index = block["index"]
                self.last_hash = block["block_hash"]
                with self.seen_lock:
                    self.seen_events.add(block["event_id"])
                self.block_hash_index[block["block_hash"]] = block["index"]
        self.logger.info(f"WAL loaded: {self.last_index} blocks.")

    def append_wal(self, block):
        """
        Append a block to the Write-Ahead Log using append-only strategy.

        This method uses direct append with fsync for durability and performance.
        No temporary file or full rewrite is needed, making it efficient for large chains.

        Creates WAL file if it doesn't exist.

        Args:
            block (dict): The block to append to the WAL
        """
        # Ensure WAL file exists, create if needed
        if not os.path.exists(WAL_PATH):
            with open(WAL_PATH, "w") as f:
                pass  # Create empty file

        # Open in append mode and write the new block
        with open(WAL_PATH, "a") as f:
            f.write(json.dumps(block) + "\n")
            f.flush()
            os.fsync(f.fileno())

        self.logger.debug(f"WAL append block {block['index']}")

    # ======================
    # Event
    # ======================
    def _event_key(self, payload):
        """
        Generate event key for hashing.

        Used by both event_id() and select_leader() to ensure consistency.

        Args:
            payload (dict): Event data

        Returns:
            str: Event key string
        """
        return f"{payload.get('inode')}:{payload.get('content_hash')}:{payload.get('event')}"

    def event_id(self, payload):
        """
        Generate a unique identifier for a filesystem event.

        The event ID is computed from inode, content hash, and event type.
        This ensures the same event (e.g., creating the same file twice)
        has the same ID for deduplication purposes.

        Args:
            payload (dict): Event data containing inode, content_hash, and event

        Returns:
            str: SHA256 hash of the event as a unique identifier
        """
        key = self._event_key(payload)
        return hashlib.sha256(key.encode()).hexdigest()

    def load_seen_events(self):
        """
        Load seen events from persistent storage.

        Restores the set of event IDs that have already been processed.
        This prevents duplicate processing of events after node restarts.

        Thread-safe via seen_lock.
        """
        if os.path.exists(SEEN_EVENTS_PATH):
            try:
                with open(SEEN_EVENTS_PATH) as f:
                    data = json.load(f)
                    with self.seen_lock:
                        self.seen_events = set(data.get("events", []))
                    self.logger.info(f"Loaded {len(self.seen_events)} seen events")
            except Exception as e:
                self.logger.warning(f"Failed to load seen events: {e}")

    def persist_seen_events(self):
        """
        Persist seen events to disk atomically.

        Uses temp file + fsync + atomic rename to ensure durability.
        This ensures event deduplication survives crashes and restarts.

        Thread-safe via seen_lock.
        """
        try:
            temp_path = SEEN_EVENTS_PATH + ".tmp"
            with self.seen_lock:
                events_list = list(self.seen_events)

            with open(temp_path, "w") as f:
                json.dump({"events": events_list}, f)
                f.flush()
                os.fsync(f.fileno())

            os.replace(temp_path, SEEN_EVENTS_PATH)
        except Exception as e:
            self.logger.error(f"Failed to persist seen events: {e}")

    # ======================
    # Block logic
    # ======================
    def compute_block_hash(self, block):
        """
        Compute the cryptographic hash of a block.

        The hash includes all block fields except the hash itself.
        This creates an immutable chain where each block references
        the hash of the previous block.

        Args:
            block (dict): Block to hash

        Returns:
            str: SHA256 hash of the block
        """
        b = dict(block)
        b.pop("block_hash", None)
        return hashlib.sha256(json.dumps(b, sort_keys=True).encode()).hexdigest()

    def create_block(self, payload):
        """
        Create a new block from an event payload.

        Constructs a block containing:
        - Current term for conflict resolution
        - Event details (type, path, inode, size, hash)
        - Link to previous block (prev_block_hash)
        - Node signature
        - Computed block hash for integrity

        Args:
            payload (dict): Event data from watchdog

        Returns:
            dict: New block ready for consensus
        """
        eid = self.event_id(payload)
        block = {
            "term": self.current_term,
            "block_version": 1,
            "timestamp": time.time(),
            "index": self.last_index + 1,
            "node_id": NODE_ID,
            "event_id": eid,
            "event": payload.get("event"),
            "path": payload.get("path"),
            "inode": payload.get("inode"),
            "size_bytes": payload.get("size_bytes"),
            "content_hash": payload.get("content_hash"),
            "prev_block_hash": self.last_hash,
            "metadata": payload.get("metadata", {}),
            "signature": self.node_key,
        }
        block["block_hash"] = self.compute_block_hash(block)
        self.logger.info(f"Created block {block['index']} {block['block_hash']}")
        return block

    def validate_block(self, block):
        """
        Validate a block for addition to the blockchain.

        Checks:
        1. Block index is sequential (last_index + 1)
        2. prev_block_hash matches current chain tip
        3. block_hash is correctly computed

        Args:
            block (dict): Block to validate

        Returns:
            bool: True if block is valid, False otherwise
        """
        if block["index"] != self.last_index + 1:
            return False
        if block["prev_block_hash"] != self.last_hash:
            return False
        if block["block_hash"] != self.compute_block_hash(block):
            return False
        return True

    def has_block(self, block_hash):
        """
        Check if a block exists in the blockchain.

        Uses O(1) hash index lookup for efficiency.

        Args:
            block_hash (str): Hash of the block to check

        Returns:
            bool: True if block exists, False otherwise
        """
        return block_hash in self.block_hash_index

    # ======================
    # Leader selection
    # ======================
    def ping_host(self, ip):
        """
        Check if a host is alive and responsive.

        Args:
            ip (str): IP address to ping

        Returns:
            bool: True if host is responsive, False otherwise
        """
        if ip == NODE_IP:
            return True
        try:
            return (
                requests.get(f"http://{ip}:5000/health", timeout=2).status_code == 200
            )
        except Exception:
            return False

    def select_leader(self, event):
        """
        Custom leader selection algorithm based on event affinity and node health.

        This implements a deterministic but dynamic leader selection:
        1. Hash the event to determine event affinity
        2. Check node health status via ping
        3. Select first healthy node in rotation
        4. Consider term numbers to prefer higher-term nodes (break ties)

        This approach ensures:
        - Same event always starts with same candidate (reduces conflicts)
        - Automatic failover if primary candidate is down
        - Term-based prioritization for conflict resolution

        Args:
            event (dict): Event payload containing inode and content_hash

        Returns:
            str: IP address of selected leader node
        """
        # Handle empty cluster
        if not CLUSTER_IPS:
            return NODE_IP

        # Create event affinity hash for deterministic starting point
        key = self._event_key(event)
        affinity_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        start_idx = affinity_hash % len(CLUSTER_IPS)

        # Try to get term information from all nodes to prioritize high-term nodes
        # Store original index to avoid O(n) lookup during sort
        node_health = []
        for i in range(len(CLUSTER_IPS)):
            ip = CLUSTER_IPS[(start_idx + i) % len(CLUSTER_IPS)]
            original_idx = (start_idx + i) % len(CLUSTER_IPS)
            try:
                if ip == NODE_IP:
                    node_health.append((ip, self.current_term, True, original_idx))
                else:
                    r = requests.get(f"http://{ip}:5000/health", timeout=2)
                    if r.status_code == 200:
                        term = r.json().get("term", 0)
                        node_health.append((ip, term, True, original_idx))
                    else:
                        node_health.append((ip, 0, False, original_idx))
            except Exception:
                node_health.append((ip, 0, False, original_idx))

        # Sort by: 1) health (alive first), 2) term (higher term first), 3) original order
        node_health.sort(key=lambda x: (-x[2], -x[1], x[3]))

        # Select first healthy node with highest term
        for ip, term, is_healthy, _ in node_health:
            if is_healthy:
                self.logger.debug(
                    f"Leader selected: {ip} (term={term}, affinity_hash={affinity_hash % 1000})"
                )
                return ip

        # Fallback to self if no other node is available
        self.logger.warning("No healthy cluster nodes found, falling back to self")
        return NODE_IP

    # ======================
    # Consensus
    # ======================
    def quorum_size(self):
        """
        Calculate the quorum size needed for consensus.

        Uses majority quorum: (N // 2) + 1 where N is cluster size.
        This ensures that any two quorums overlap, preventing split decisions.

        Returns:
            int: Number of nodes needed for quorum
        """
        # Handle empty cluster
        if not CLUSTER_IPS:
            return 1
        return len(CLUSTER_IPS) // 2 + 1

    def commit_block(self, block):
        """
        Commit a block to the blockchain using quorum consensus.

        Process:
        1. Validate the block
        2. Replicate to all cluster nodes
        3. Count acknowledgments (including self)
        4. If quorum reached, apply block; otherwise bump term

        Args:
            block (dict): Block to commit

        Returns:
            bool: True if block committed successfully, False if quorum not reached
        """
        if not self.validate_block(block):
            return False

        acks = 1
        failed_nodes = []
        for ip in CLUSTER_IPS:
            if ip == NODE_IP:
                continue
            try:
                r = requests.post(f"http://{ip}:5000/replicate", json=block, timeout=3)
                if r.status_code == 200:
                    acks += 1
                else:
                    failed_nodes.append(ip)
                    self.logger.warning(
                        f"Replication to {ip} failed with status {r.status_code}: {r.text[:100]}"
                    )
            except Exception as e:
                failed_nodes.append(ip)
                self.logger.debug(f"Replication failed to {ip}: {e}")

        if acks < self.quorum_size():
            self.logger.warning(
                f"Quorum not reached: {acks}/{self.quorum_size()}, failed nodes: {failed_nodes}"
            )
            self.bump_term()
            return False

        self.apply_block(block)
        return True

    def apply_block(self, block):
        """
        Apply a block to the blockchain (WAL-first approach).

        This method MUST be called within append_lock for thread safety.

        Steps:
        1. Check if block already exists (idempotency)
        2. Write to WAL (disk) first for durability
        3. Update in-memory state (chain, indices, seen events)
        4. Periodically persist seen events

        Args:
            block (dict): Block to apply
        """
        # This function MUST be called within append_lock
        if self.has_block(block["block_hash"]):
            return

        # WAL-first approach: write to disk before applying to memory
        self.append_wal(block)

        # Now update in-memory state
        self.chain.append(block)
        self.last_index = block["index"]
        self.last_hash = block["block_hash"]
        self.block_hash_index[block["block_hash"]] = block["index"]

        with self.seen_lock:
            self.seen_events.add(block["event_id"])

        # Periodically persist seen events (configurable via SEEN_EVENTS_PERSIST_INTERVAL)
        if block["index"] % SEEN_EVENTS_PERSIST_INTERVAL == 0:
            self.persist_seen_events()

    def propose_and_commit(self, payload):
        """
        Propose a new event and commit it to the blockchain.

        Implements double-check locking pattern for thread safety:
        1. Quick check if event already processed (without lock)
        2. Acquire append_lock for exclusive access
        3. Double-check event hasn't been processed while waiting for lock
        4. Create block and attempt consensus
        5. Queue for retry if consensus fails

        Args:
            payload (dict): Event data from watchdog

        Returns:
            dict or None: Block if successfully committed, None if duplicate or failed
        """
        eid = self.event_id(payload)

        # Thread-safe duplicate check
        with self.seen_lock:
            if eid in self.seen_events:
                self.logger.debug(f"Event {eid} already processed, skipping")
                return None

        # Acquire lock for entire block creation and commit process
        with self.append_lock:
            # Double-check after acquiring lock (another thread might have processed it)
            with self.seen_lock:
                if eid in self.seen_events:
                    self.logger.debug(
                        f"Event {eid} already processed (double-check), skipping"
                    )
                    return None

            block = self.create_block(payload)
            if self.commit_block(block):
                return block

            # Failed to commit, queue for retry
            with self.retry_lock:
                self.retry_queue.append(payload)
            return None

    # ======================
    # Sync
    # ======================
    def sync_blockchain(self):
        """
        Synchronize blockchain state with other nodes.

        This method:
        1. Queries all cluster nodes for their blockchains
        2. Identifies missing blocks
        3. Sorts blocks by index
        4. Applies blocks sequentially with validation
        5. Detects and logs gaps in the chain

        Called periodically by background sync_loop thread.
        Thread-safe via append_lock.
        """
        for ip in CLUSTER_IPS:
            try:
                r = requests.get(f"http://{ip}:5000/get_blocks", timeout=3)
                if r.status_code != 200:
                    continue

                remote_blocks = r.json()

                # Build a dict of blocks we don't have
                missing_blocks = []
                for block in remote_blocks:
                    if not self.has_block(block["block_hash"]):
                        missing_blocks.append(block)

                # Sort by index to apply in order
                missing_blocks.sort(key=lambda b: b["index"])

                # Try to apply blocks in sequence
                with self.append_lock:
                    for block in missing_blocks:
                        # Only apply blocks that follow our current chain
                        if block["index"] == self.last_index + 1:
                            # Validate hash chain integrity
                            if block["prev_block_hash"] == self.last_hash:
                                self.apply_block(block)
                                self.logger.info(
                                    f"Synced block {block['index']} from {ip}"
                                )
                        elif block["index"] > self.last_index + 1:
                            # Gap detected, need to sync more blocks
                            self.logger.warning(
                                f"Gap detected: have {self.last_index}, remote has {block['index']}"
                            )
                            break

            except Exception as e:
                self.logger.debug(f"Sync failed from {ip}: {e}")


bc = Blockchain()


# ======================
# Routes
# ======================
@app.post("/event")
def receive_event():
    """
    Receive a filesystem event from watchdog.

    Workflow:
    1. Select leader based on event affinity
    2. If not leader, return leader info (client should retry with leader)
    3. If leader, propose and commit the event

    POST /event
    Body: {
        "event": "create|modify|delete|move",
        "path": "/path/to/file.qcow2",
        "inode": 12345,
        "size_bytes": 1024,
        "content_hash": "sha256...",
        "metadata": {}
    }

    Returns:
        200: {"status": "committed"} - Block added to blockchain
        200: {"status": "queued"} - Failed consensus, queued for retry
        200: {"status": "ignored", "leader": "ip"} - Not leader, redirect to leader
    """
    payload = request.get_json(force=True)
    leader = bc.select_leader(payload)
    if leader != NODE_IP:
        return jsonify({"status": "ignored", "leader": leader}), 200

    block = bc.propose_and_commit(payload)
    return jsonify({"status": "committed" if block else "queued"}), 200


@app.post("/replicate")
def replicate():
    """
    Replicate a block from another node (consensus protocol).

    Called by leader node to replicate blocks to follower nodes.
    Thread-safe with proper locking.

    POST /replicate
    Body: {block object}

    Returns:
        200: {"status": "ok"} - Block accepted and applied
        200: {"status": "exists"} - Block already exists
        400: {"error": "invalid"} - Block validation failed
    """
    block = request.get_json(force=True)

    # Thread-safe check and apply
    with bc.append_lock:
        if bc.has_block(block["block_hash"]):
            return jsonify({"status": "exists"}), 200

        if bc.validate_block(block):
            bc.apply_block(block)
            return jsonify({"status": "ok"}), 200

    return jsonify({"error": "invalid"}), 400


@app.get("/get_blocks")
def get_blocks():
    """
    Get all blocks in the blockchain.

    Used for sync operations between nodes.

    GET /get_blocks

    Returns:
        200: [array of blocks]
    """
    return jsonify(bc.chain)


@app.get("/health")
def health():
    """
    Health check endpoint.

    Returns node health status and current term.
    Used by leader selection and monitoring.

    GET /health

    Returns:
        200: {"status": "healthy", "term": 42}
    """
    return jsonify({"status": "healthy", "term": bc.current_term})


# ======================
# ✅ FIXED STATUS PAGE
# ======================
@app.get("/status")
def status_page():
    block_map = {}

    def record(block, node):
        h = block["block_hash"]
        if h not in block_map:
            block_map[h] = {
                "block": block,
                "nodes": set(),
            }
        block_map[h]["nodes"].add(node)

    # Query ALL nodes (including self)
    for ip in set(CLUSTER_IPS + [NODE_IP]):
        try:
            r = requests.get(f"http://{ip}:5000/get_blocks", timeout=3)
            if r.status_code == 200:
                for block in r.json():
                    record(block, ip)
        except Exception:
            bc.logger.warning(f"Status: failed to query {ip}")

    blocks = []
    for entry in block_map.values():
        b = entry["block"].copy()
        b["nodes_with_block"] = sorted(entry["nodes"])
        b["formatted_timestamp"] = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(b["timestamp"])
        )
        blocks.append(b)

    blocks.sort(key=lambda x: (x["index"], x["term"]))

    return render_template(
        "status.html",
        node_id=NODE_ID,
        cluster_nodes=CLUSTER_IPS,
        blocks=blocks,
        total_blocks=len(blocks),
        last_index=bc.last_index,
        last_hash=bc.last_hash,
    )


# ======================
# Background threads
# ======================
def retry_loop():
    while True:
        time.sleep(2)
        with bc.retry_lock:
            if bc.retry_queue:
                payload = bc.retry_queue.pop(0)
            else:
                payload = None

        if payload:
            bc.logger.info(f"Retrying event: {payload.get('path')}")
            bc.propose_and_commit(payload)


def sync_loop():
    while True:
        time.sleep(5)
        bc.sync_blockchain()


def persist_seen_events_loop():
    """Periodically persist seen events to disk"""
    while True:
        time.sleep(30)  # Persist every 30 seconds
        bc.persist_seen_events()


# ======================
# Main
# ======================
if __name__ == "__main__":
    threading.Thread(target=retry_loop, daemon=True).start()
    threading.Thread(target=sync_loop, daemon=True).start()
    threading.Thread(target=persist_seen_events_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
