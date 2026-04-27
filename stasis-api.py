#!/usr/bin/env python3
import os
import json
import random
import time
import hashlib
import threading
import logging
import requests
from flask import Flask, request, jsonify, render_template
from stasis_discovery import UDPDiscovery

# ======================
# Environment
# ======================
NODE_ID = os.environ.get("NODE_ID", "node-1")
NODE_IP = os.environ.get("NODE_IP", "127.0.0.1")

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
# UDP Discovery (global)
# ======================
discovery = UDPDiscovery(node_id=NODE_ID, node_ip=NODE_IP)


# ======================
# Raft Leader Election
# ======================
class RaftRole:
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    """
    Simplified Raft-inspired leader election.

    Roles:
    - FOLLOWER:  default state; resets timeout on every heartbeat from leader.
    - CANDIDATE: started election; collecting votes.
    - LEADER:    won election; sends heartbeats to peers.

    Endpoints expected on peers:
    - POST /raft/vote       {"term", "candidate_id", "candidate_ip"}
    - POST /raft/heartbeat  {"term", "leader_ip", "leader_id"}
    """

    ELECTION_TIMEOUT_MIN = 4.0  # seconds
    ELECTION_TIMEOUT_MAX = 8.0  # seconds
    HEARTBEAT_INTERVAL = 1.5  # seconds

    def __init__(self, node_id: str, node_ip: str, disc: UDPDiscovery):
        self.node_id = node_id
        self.node_ip = node_ip
        self.discovery = disc
        self.logger = logging.getLogger("raft")

        self._lock = threading.Lock()
        self.role = RaftRole.FOLLOWER
        self.current_term = 0
        self.voted_for: dict = {}  # term -> node_id
        self.leader_ip: str | None = None
        self._last_heartbeat = time.time()
        self._election_timeout = self._rand_timeout()
        self._running = False

    def _rand_timeout(self) -> float:
        return random.uniform(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, initial_term: int = 0):
        """Start background Raft threads."""
        with self._lock:
            self.current_term = initial_term
        self._running = True
        threading.Thread(
            target=self._election_timer_loop, daemon=True, name="raft-election"
        ).start()
        threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="raft-heartbeat"
        ).start()
        self.logger.info("Raft started: term=%d", initial_term)

    def stop(self):
        self._running = False

    def is_leader(self) -> bool:
        with self._lock:
            return self.role == RaftRole.LEADER

    def get_leader_ip(self) -> str | None:
        with self._lock:
            return self.leader_ip

    def get_term(self) -> int:
        with self._lock:
            return self.current_term

    def set_term(self, term: int):
        """Called by Blockchain to sync Raft term with persisted term."""
        with self._lock:
            if term > self.current_term:
                self.current_term = term

    def get_status(self) -> dict:
        with self._lock:
            return {
                "role": self.role,
                "term": self.current_term,
                "leader_ip": self.leader_ip,
                "node_id": self.node_id,
                "node_ip": self.node_ip,
            }

    def receive_heartbeat(self, term: int, leader_ip: str, leader_id: str) -> bool:
        """Process an incoming leader heartbeat. Returns True if accepted."""
        with self._lock:
            if term < self.current_term:
                return False
            self.current_term = term
            self.role = RaftRole.FOLLOWER
            self.leader_ip = leader_ip
            self._last_heartbeat = time.time()
            self._election_timeout = self._rand_timeout()
            return True

    def receive_vote_request(
        self, term: int, candidate_id: str, candidate_ip: str
    ) -> dict:
        """Process an incoming vote request. Returns vote response dict."""
        with self._lock:
            if term < self.current_term:
                return {"vote_granted": False, "term": self.current_term}
            if term > self.current_term:
                self.current_term = term
                self.role = RaftRole.FOLLOWER
                self.voted_for = {}
            if term not in self.voted_for or self.voted_for[term] == candidate_id:
                self.voted_for[term] = candidate_id
                self._last_heartbeat = time.time()
                return {"vote_granted": True, "term": self.current_term}
            return {"vote_granted": False, "term": self.current_term}

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    def _election_timer_loop(self):
        while self._running:
            time.sleep(0.5)
            with self._lock:
                if self.role == RaftRole.LEADER:
                    continue
                elapsed = time.time() - self._last_heartbeat
                if elapsed < self._election_timeout:
                    continue
                self.current_term += 1
                self.role = RaftRole.CANDIDATE
                self.voted_for[self.current_term] = self.node_id
                term = self.current_term
                self._last_heartbeat = time.time()
                self._election_timeout = self._rand_timeout()

            self.logger.info("Election timeout — starting election for term %d", term)
            self._run_election(term)

    def _run_election(self, term: int):
        peers = self.discovery.get_peers()
        votes = 1  # self-vote
        quorum = (len(peers) + 1) // 2 + 1

        for peer_ip in peers:
            try:
                r = requests.post(
                    f"http://{peer_ip}:5000/raft/vote",
                    json={
                        "term": term,
                        "candidate_id": self.node_id,
                        "candidate_ip": self.node_ip,
                    },
                    timeout=2,
                )
                if r.status_code == 200:
                    resp = r.json()
                    if resp.get("vote_granted") and resp.get("term") == term:
                        votes += 1
                    elif resp.get("term", 0) > term:
                        with self._lock:
                            self.current_term = resp["term"]
                            self.role = RaftRole.FOLLOWER
                            self._last_heartbeat = time.time()
                        return
            except Exception as exc:
                self.logger.debug("Vote request to %s failed: %s", peer_ip, exc)

        with self._lock:
            if self.role == RaftRole.CANDIDATE and self.current_term == term:
                if votes >= quorum:
                    self.role = RaftRole.LEADER
                    self.leader_ip = self.node_ip
                    self.logger.info(
                        "Elected LEADER for term %d (%d/%d votes)",
                        term,
                        votes,
                        len(peers) + 1,
                    )
                else:
                    self.role = RaftRole.FOLLOWER
                    self._election_timeout = self._rand_timeout()
                    self._last_heartbeat = time.time()
                    self.logger.info(
                        "Election lost for term %d (%d/%d needed)", term, votes, quorum
                    )

    def _heartbeat_loop(self):
        while self._running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            with self._lock:
                if self.role != RaftRole.LEADER:
                    continue
                term = self.current_term

            for peer_ip in self.discovery.get_peers():
                try:
                    requests.post(
                        f"http://{peer_ip}:5000/raft/heartbeat",
                        json={
                            "term": term,
                            "leader_ip": self.node_ip,
                            "leader_id": self.node_id,
                        },
                        timeout=1,
                    )
                except Exception:
                    pass


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
    # Consensus
    # ======================
    def quorum_size(self, peers: list) -> int:
        """
        Calculate the quorum size needed for consensus.

        Uses majority quorum: (N // 2) + 1 where N is total cluster size.
        This ensures that any two quorums overlap, preventing split decisions.

        Args:
            peers: List of peer IPs from discovery (excludes self)

        Returns:
            int: Number of nodes needed for quorum
        """
        total = len(peers) + 1  # peers + self
        return total // 2 + 1

    def commit_block(self, block):
        """
        Commit a block to the blockchain using quorum consensus.

        Process:
        1. Validate the block
        2. Replicate to all discovered peers
        3. Count acknowledgments (including self)
        4. If quorum reached, apply block; otherwise bump term

        Args:
            block (dict): Block to commit

        Returns:
            bool: True if block committed successfully, False if quorum not reached
        """
        if not self.validate_block(block):
            return False

        peers = discovery.get_peers()
        acks = 1
        failed_nodes = []
        for ip in peers:
            try:
                r = requests.post(f"http://{ip}:5000/replicate", json=block, timeout=3)
                if r.status_code == 200:
                    acks += 1
                else:
                    failed_nodes.append(ip)
                    self.logger.warning(
                        "Replication to %s failed (%d): %s",
                        ip,
                        r.status_code,
                        r.text[:100],
                    )
            except Exception as exc:
                failed_nodes.append(ip)
                self.logger.debug("Replication failed to %s: %s", ip, exc)

        needed = self.quorum_size(peers)
        if acks < needed:
            self.logger.warning(
                "Quorum not reached: %d/%d, failed nodes: %s",
                acks,
                needed,
                failed_nodes,
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
        Synchronize blockchain state with discovered peers.

        Queries all live peers for blocks this node is missing,
        applies them in sequential index order with hash-chain validation.

        If this node has no blocks yet (fresh start), triggers a full
        bootstrap sync from the peer with the longest chain.

        Called periodically by the background sync_loop thread.
        Thread-safe via append_lock.
        """
        peers = discovery.get_peers()
        if not peers:
            return

        # Bootstrap: if chain is empty, do a full sync from the longest peer
        if self.last_index == 0:
            self._bootstrap_from_peers(peers)
            return

        for ip in peers:
            try:
                # Only request blocks we are missing (saves bandwidth)
                r = requests.get(
                    f"http://{ip}:5000/get_blocks",
                    params={"since": self.last_index},
                    timeout=5,
                )
                if r.status_code != 200:
                    continue

                remote_blocks = [
                    b for b in r.json() if not self.has_block(b["block_hash"])
                ]
                remote_blocks.sort(key=lambda b: b["index"])

                with self.append_lock:
                    for block in remote_blocks:
                        if block["index"] == self.last_index + 1:
                            if block["prev_block_hash"] == self.last_hash:
                                self.apply_block(block)
                                self.logger.info(
                                    "Synced block %d from %s", block["index"], ip
                                )
                        elif block["index"] > self.last_index + 1:
                            self.logger.warning(
                                "Gap detected: have %d, remote has %d",
                                self.last_index,
                                block["index"],
                            )
                            break

            except Exception as exc:
                self.logger.debug("Sync failed from %s: %s", ip, exc)

    def _bootstrap_from_peers(self, peers: list):
        """
        Download the full blockchain from the peer with the longest chain.

        Called once on startup when the local chain is empty.
        Thread-safe via append_lock.
        """
        best_ip = None
        best_len = 0
        for ip in peers:
            try:
                r = requests.get(f"http://{ip}:5000/health", timeout=2)
                if r.status_code == 200:
                    length = r.json().get("chain_length", 0)
                    if length > best_len:
                        best_len = length
                        best_ip = ip
            except Exception:
                pass

        if not best_ip or best_len == 0:
            return

        try:
            r = requests.get(f"http://{best_ip}:5000/sync_full", timeout=30)
            if r.status_code != 200:
                return
            blocks = r.json()
            self.logger.info("Bootstrapping %d blocks from %s", len(blocks), best_ip)
            with self.append_lock:
                for block in blocks:
                    if not self.has_block(block["block_hash"]):
                        if self.validate_block(block):
                            self.apply_block(block)
                        else:
                            self.logger.warning(
                                "Invalid block %d during bootstrap, aborting",
                                block["index"],
                            )
                            break
        except Exception as exc:
            self.logger.warning("Bootstrap from %s failed: %s", best_ip, exc)


bc = Blockchain()
raft = RaftNode(node_id=NODE_ID, node_ip=NODE_IP, disc=discovery)


# ======================
# Routes — Raft
# ======================
@app.post("/raft/vote")
def raft_vote():
    """
    Handle a Raft vote request from a candidate.

    POST /raft/vote
    Body: {"term": int, "candidate_id": str, "candidate_ip": str}

    Returns:
        200: {"vote_granted": bool, "term": int}
    """
    data = request.get_json(force=True)
    result = raft.receive_vote_request(
        term=data["term"],
        candidate_id=data["candidate_id"],
        candidate_ip=data["candidate_ip"],
    )
    return jsonify(result), 200


@app.post("/raft/heartbeat")
def raft_heartbeat():
    """
    Receive a heartbeat from the current Raft leader.

    POST /raft/heartbeat
    Body: {"term": int, "leader_ip": str, "leader_id": str}

    Returns:
        200: {"accepted": bool}
    """
    data = request.get_json(force=True)
    accepted = raft.receive_heartbeat(
        term=data["term"],
        leader_ip=data["leader_ip"],
        leader_id=data["leader_id"],
    )
    return jsonify({"accepted": accepted}), 200


# ======================
# Routes — Blockchain
# ======================
@app.post("/event")
def receive_event():
    """
    Receive a filesystem event from watchdog.

    If this node is not the current Raft leader, redirect the client to the
    leader IP so it can retry there.  If no leader is known yet, accept
    the event anyway (single-node or early-bootstrap scenario).

    POST /event
    Body: {
        "event": "create|modify|delete|move",
        "path": "/path/to/file.qcow2",
        "inode": 12345,
        "size_bytes": 1024,
        "content_hash": "blake3...",
        "metadata": {}
    }

    Returns:
        200: {"status": "committed"} – block added to blockchain
        200: {"status": "queued"}    – consensus failed, queued for retry
        200: {"status": "redirect", "leader": "ip"} – not leader, retry there
    """
    payload = request.get_json(force=True)
    leader_ip = raft.get_leader_ip()

    # Redirect to leader if one is known and it is not us
    if leader_ip and leader_ip != NODE_IP:
        return jsonify({"status": "redirect", "leader": leader_ip}), 200

    block = bc.propose_and_commit(payload)
    return jsonify({"status": "committed" if block else "queued"}), 200


@app.post("/replicate")
def replicate():
    """
    Replicate a block from the leader (consensus protocol).

    Validates the block hash before accepting.  On corrupt hash, returns
    409 so the sender knows this node has detected an integrity violation.

    POST /replicate
    Body: {block object}

    Returns:
        200: {"status": "ok"}     – block accepted and applied
        200: {"status": "exists"} – block already present
        400: {"error": "invalid", "reason": str} – validation failed
        409: {"error": "corrupt_hash", "block_index": int} – hash mismatch
    """
    block = request.get_json(force=True)

    # Quick integrity check before acquiring the lock
    computed = bc.compute_block_hash(block)
    if computed != block.get("block_hash"):
        bc.logger.error(
            "Corrupt block hash received for index %s (got %s, expected %s)",
            block.get("index"),
            block.get("block_hash", "")[:16],
            computed[:16],
        )
        return jsonify(
            {"error": "corrupt_hash", "block_index": block.get("index")}
        ), 409

    with bc.append_lock:
        if bc.has_block(block["block_hash"]):
            return jsonify({"status": "exists"}), 200

        if bc.validate_block(block):
            bc.apply_block(block)
            return jsonify({"status": "ok"}), 200

    return jsonify({"error": "invalid", "reason": "chain validation failed"}), 400


@app.get("/get_blocks")
def get_blocks():
    """
    Return blocks in the blockchain, optionally filtered by index.

    GET /get_blocks?since=<index>

    Returns:
        200: [array of blocks with index > since]
    """
    since = request.args.get("since", 0, type=int)
    chain = bc.chain
    if since:
        chain = [b for b in chain if b["index"] > since]
    return jsonify(chain)


@app.get("/sync_full")
def sync_full():
    """
    Return the complete blockchain for a bootstrapping node.

    GET /sync_full

    Returns:
        200: [full array of blocks]
    """
    return jsonify(bc.chain)


@app.get("/health")
def health():
    """
    Health check endpoint.

    Returns node health including Raft role, current term, leader, and chain length.

    GET /health

    Returns:
        200: {"status": "healthy", "term": 42, "role": "leader",
              "leader_ip": "...", "chain_length": 100}
    """
    raft_status = raft.get_status()
    return jsonify(
        {
            "status": "healthy",
            "node_id": NODE_ID,
            "term": bc.current_term,
            "role": raft_status["role"],
            "leader_ip": raft_status["leader_ip"],
            "chain_length": bc.last_index,
        }
    )


@app.get("/cluster_status")
def cluster_status():
    """
    Return cluster health summary for the UI.

    Queries each discovered peer's /health endpoint.  Unreachable peers are
    reported as "not_ready".

    GET /cluster_status

    Returns:
        200: {
            "nodes": [{"node_id", "ip", "status", "role", "term", "chain_length"}],
            "total": int,
            "healthy": int,
            "leader_ip": str | null
        }
    """
    nodes = []

    # Self
    raft_status = raft.get_status()
    nodes.append(
        {
            "node_id": NODE_ID,
            "ip": NODE_IP,
            "status": "ready",
            "role": raft_status["role"],
            "term": bc.current_term,
            "chain_length": bc.last_index,
        }
    )

    # Peers
    peer_details = discovery.get_peer_details()
    for ip, info in peer_details.items():
        if ip == NODE_IP:
            continue
        try:
            r = requests.get(f"http://{ip}:5000/health", timeout=2)
            if r.status_code == 200:
                data = r.json()
                nodes.append(
                    {
                        "node_id": info.get("node_id", ip),
                        "ip": ip,
                        "status": "ready",
                        "role": data.get("role", "unknown"),
                        "term": data.get("term", 0),
                        "chain_length": data.get("chain_length", 0),
                    }
                )
            else:
                nodes.append(
                    {
                        "node_id": info.get("node_id", ip),
                        "ip": ip,
                        "status": "not_ready",
                        "role": "unknown",
                        "term": 0,
                        "chain_length": 0,
                    }
                )
        except Exception:
            nodes.append(
                {
                    "node_id": info.get("node_id", ip),
                    "ip": ip,
                    "status": "not_ready",
                    "role": "unknown",
                    "term": 0,
                    "chain_length": 0,
                }
            )

    healthy = sum(1 for n in nodes if n["status"] == "ready")
    leader_ip = raft.get_leader_ip()

    return jsonify(
        {
            "nodes": sorted(nodes, key=lambda n: n["node_id"]),
            "total": len(nodes),
            "healthy": healthy,
            "leader_ip": leader_ip,
        }
    )


# ======================
# Status page
# ======================
@app.get("/status")
def status_page():
    block_map = {}

    def record(block, node):
        h = block["block_hash"]
        if h not in block_map:
            block_map[h] = {"block": block, "nodes": set()}
        block_map[h]["nodes"].add(node)

    # Query all peers (including self)
    all_ips = set(discovery.get_peers() + [NODE_IP])
    for ip in all_ips:
        try:
            r = requests.get(f"http://{ip}:5000/get_blocks", timeout=3)
            if r.status_code == 200:
                for block in r.json():
                    record(block, ip)
        except Exception:
            bc.logger.warning("Status: failed to query %s", ip)

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
            payload = bc.retry_queue.pop(0) if bc.retry_queue else None
        if payload:
            bc.logger.info("Retrying event: %s", payload.get("path"))
            bc.propose_and_commit(payload)


def sync_loop():
    while True:
        time.sleep(5)
        bc.sync_blockchain()


def persist_seen_events_loop():
    """Periodically flush seen events to disk."""
    while True:
        time.sleep(30)
        bc.persist_seen_events()


# ======================
# Main
# ======================
if __name__ == "__main__":
    # Start UDP peer discovery
    discovery.start()

    # Start Raft leader election (sync term from persisted blockchain state)
    raft.start(initial_term=bc.current_term)

    # Background blockchain threads
    threading.Thread(target=retry_loop, daemon=True).start()
    threading.Thread(target=sync_loop, daemon=True).start()
    threading.Thread(target=persist_seen_events_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=5000)
