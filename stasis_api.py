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

NODE_ID = os.environ.get("NODE_ID", "node-1")
NODE_IP = os.environ.get("NODE_IP", "127.0.0.1")

DATA_DIR = os.environ.get("DATA_DIR", "./data")
WAL_PATH = os.path.join(DATA_DIR, "blockchain.wal")
TERM_PATH = os.path.join(DATA_DIR, "term.json")
SEEN_EVENTS_PATH = os.path.join(DATA_DIR, "seen_events.json")

SEEN_EVENTS_PERSIST_INTERVAL = int(os.environ.get("SEEN_EVENTS_PERSIST_INTERVAL", "10"))

os.makedirs(DATA_DIR, exist_ok=True)

app = Flask(__name__)

discovery = UDPDiscovery(node_id=NODE_ID, node_ip=NODE_IP)


class RaftRole:
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    ELECTION_TIMEOUT_MIN = 4.0
    ELECTION_TIMEOUT_MAX = 8.0
    HEARTBEAT_INTERVAL = 1.5

    def __init__(self, node_id: str, node_ip: str, disc: UDPDiscovery):
        self.node_id = node_id
        self.node_ip = node_ip
        self.discovery = disc
        self.logger = logging.getLogger("raft")

        self._lock = threading.Lock()
        self.role = RaftRole.FOLLOWER
        self.current_term = 0
        self.voted_for: dict = {}
        self.leader_ip: str | None = None
        self._last_heartbeat = time.time()
        self._election_timeout = self._rand_timeout()
        self._running = False

    def _rand_timeout(self) -> float:
        return random.uniform(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)

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


class Blockchain:
    def __init__(self):
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

        self.append_lock = threading.Lock()
        self.term_lock = threading.Lock()
        self.retry_lock = threading.Lock()
        self.seen_lock = threading.Lock()
        self.retry_queue = []
        self.seen_events = set()
        self.block_hash_index = {}
        self.load_term()
        self.load_wal()
        self.load_seen_events()

    def load_term(self):
        with self.term_lock:
            if os.path.exists(TERM_PATH):
                with open(TERM_PATH) as f:
                    self.current_term = json.load(f)["term"]
            else:
                self.persist_term()

    def persist_term(self):
        temp_path = TERM_PATH + ".tmp"
        with open(temp_path, "w") as f:
            json.dump({"term": self.current_term}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, TERM_PATH)

    def bump_term(self):
        with self.term_lock:
            self.current_term += 1
            self.persist_term()
            self.logger.warning(f"Bumped term to {self.current_term}")

    def load_wal(self):
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
        if not os.path.exists(WAL_PATH):
            with open(WAL_PATH, "w") as f:
                pass

        with open(WAL_PATH, "a") as f:
            f.write(json.dumps(block) + "\n")
            f.flush()
            os.fsync(f.fileno())

        self.logger.debug(f"WAL append block {block['index']}")

    def _event_key(self, payload):
        return f"{payload.get('inode')}:{payload.get('content_hash')}:{payload.get('event')}"

    def event_id(self, payload):
        key = self._event_key(payload)
        return hashlib.sha256(key.encode()).hexdigest()

    def load_seen_events(self):
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

    def compute_block_hash(self, block):
        b = dict(block)
        b.pop("block_hash", None)
        return hashlib.sha256(json.dumps(b, sort_keys=True).encode()).hexdigest()

    def create_block(self, payload):
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
        if block["index"] != self.last_index + 1:
            return False
        if block["prev_block_hash"] != self.last_hash:
            return False
        if block["block_hash"] != self.compute_block_hash(block):
            return False
        return True

    def has_block(self, block_hash):
        return block_hash in self.block_hash_index

    def quorum_size(self, peers: list) -> int:
        total = len(peers) + 1
        return total // 2 + 1

    def commit_block(self, block):
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
        if self.has_block(block["block_hash"]):
            return

        self.append_wal(block)

        self.chain.append(block)
        self.last_index = block["index"]
        self.last_hash = block["block_hash"]
        self.block_hash_index[block["block_hash"]] = block["index"]

        with self.seen_lock:
            self.seen_events.add(block["event_id"])

        if block["index"] % SEEN_EVENTS_PERSIST_INTERVAL == 0:
            self.persist_seen_events()

    def propose_and_commit(self, payload):
        eid = self.event_id(payload)

        with self.seen_lock:
            if eid in self.seen_events:
                self.logger.debug(f"Event {eid} already processed, skipping")
                return None

        with self.append_lock:
            with self.seen_lock:
                if eid in self.seen_events:
                    self.logger.debug(
                        f"Event {eid} already processed (double-check), skipping"
                    )
                    return None

            block = self.create_block(payload)
            if self.commit_block(block):
                return block

            with self.retry_lock:
                self.retry_queue.append(payload)
            return None

    def sync_blockchain(self):
        peers = discovery.get_peers()
        if not peers:
            return

        if self.last_index == 0:
            self._bootstrap_from_peers(peers)
            return

        for ip in peers:
            try:
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


@app.post("/raft/vote")
def raft_vote():
    data = request.get_json(force=True)
    result = raft.receive_vote_request(
        term=data["term"],
        candidate_id=data["candidate_id"],
        candidate_ip=data["candidate_ip"],
    )
    return jsonify(result), 200


@app.post("/raft/heartbeat")
def raft_heartbeat():
    data = request.get_json(force=True)
    accepted = raft.receive_heartbeat(
        term=data["term"],
        leader_ip=data["leader_ip"],
        leader_id=data["leader_id"],
    )
    return jsonify({"accepted": accepted}), 200


@app.post("/event")
def receive_event():
    payload = request.get_json(force=True)
    leader_ip = raft.get_leader_ip()

    if leader_ip and leader_ip != NODE_IP:
        return jsonify({"status": "redirect", "leader": leader_ip}), 200

    block = bc.propose_and_commit(payload)
    return jsonify({"status": "committed" if block else "queued"}), 200


@app.post("/replicate")
def replicate():
    block = request.get_json(force=True)

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
    since = request.args.get("since", 0, type=int)
    chain = bc.chain
    if since:
        chain = [b for b in chain if b["index"] > since]
    return jsonify(chain)


@app.get("/sync_full")
def sync_full():
    return jsonify(bc.chain)


@app.get("/health")
def health():
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
    nodes = []

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


@app.get("/status")
def status_page():
    block_map = {}

    def record(block, node):
        h = block["block_hash"]
        if h not in block_map:
            block_map[h] = {"block": block, "nodes": set()}
        block_map[h]["nodes"].add(node)

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
    while True:
        time.sleep(30)
        bc.persist_seen_events()


if __name__ == "__main__":
    discovery.start()

    raft.start(initial_term=bc.current_term)

    threading.Thread(target=retry_loop, daemon=True).start()
    threading.Thread(target=sync_loop, daemon=True).start()
    threading.Thread(target=persist_seen_events_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=5000)
