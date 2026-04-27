#!/usr/bin/env python3

import json
import logging
import os
import socket
import threading
import time

DISCOVERY_PORT = int(os.environ.get("DISCOVERY_PORT", "7000"))
DISCOVERY_INTERVAL = int(os.environ.get("DISCOVERY_INTERVAL", "5"))
PEER_TTL = int(os.environ.get("PEER_TTL", "15"))
BROADCAST_ADDR = os.environ.get("DISCOVERY_BROADCAST", "255.255.255.255")
API_PORT = int(os.environ.get("API_PORT", "5000"))


class UDPDiscovery:
    def __init__(self, node_id: str, node_ip: str):
        self.node_id = node_id
        self.node_ip = node_ip
        self.logger = logging.getLogger("discovery")

        self._peers: dict = {}
        self._lock = threading.RLock()
        self._running = False

    def start(self):
        self._running = True
        for target, name in (
            (self._broadcast_loop, "udp-broadcaster"),
            (self._listen_loop, "udp-listener"),
            (self._reaper_loop, "peer-reaper"),
        ):
            threading.Thread(target=target, daemon=True, name=name).start()

        self.logger.info(
            self.node_id,
            self.node_ip,
            DISCOVERY_PORT,
            BROADCAST_ADDR,
        )

    def stop(self):
        self._running = False

    def get_peers(self) -> list:
        with self._lock:
            now = time.time()
            return [
                ip
                for ip, info in self._peers.items()
                if ip != self.node_ip and (now - info["last_seen"]) < PEER_TTL
            ]

    def get_peer_details(self) -> dict:
        with self._lock:
            now = time.time()
            result = {}
            result[self.node_ip] = {
                "node_id": self.node_id,
                "node_ip": self.node_ip,
                "api_port": API_PORT,
                "last_seen": now,
            }
            for ip, info in self._peers.items():
                if (now - info["last_seen"]) < PEER_TTL:
                    result[ip] = info.copy()
            return result

    def _make_beacon(self) -> bytes:
        return json.dumps(
            {
                "node_id": self.node_id,
                "node_ip": self.node_ip,
                "api_port": API_PORT,
                "ts": time.time(),
            }
        ).encode()

    def _broadcast_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            while self._running:
                try:
                    sock.sendto(self._make_beacon(), (BROADCAST_ADDR, DISCOVERY_PORT))
                    self.logger.debug(
                        "Beacon sent to %s:%d", BROADCAST_ADDR, DISCOVERY_PORT
                    )
                except OSError as exc:
                    self.logger.warning("Broadcast error: %s", exc)
                time.sleep(DISCOVERY_INTERVAL)
        finally:
            sock.close()

    def _listen_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            sock.bind(("", DISCOVERY_PORT))
            sock.settimeout(2.0)
            while self._running:
                try:
                    data, addr = sock.recvfrom(4096)
                    self._handle_beacon(data, addr)
                except socket.timeout:
                    continue
                except OSError as exc:
                    self.logger.warning("Listen error: %s", exc)
        finally:
            sock.close()

    def _handle_beacon(self, data: bytes, addr):
        try:
            beacon = json.loads(data.decode())
        except Exception:
            return

        peer_ip = beacon.get("node_ip") or addr[0]
        peer_id = beacon.get("node_id", "unknown")

        with self._lock:
            is_new = peer_ip not in self._peers
            self._peers[peer_ip] = {
                "node_id": peer_id,
                "node_ip": peer_ip,
                "api_port": beacon.get("api_port", API_PORT),
                "last_seen": time.time(),
            }

        if is_new and peer_ip != self.node_ip:
            self.logger.info("New peer discovered: %s @ %s", peer_id, peer_ip)

    def _reaper_loop(self):
        while self._running:
            time.sleep(PEER_TTL)
            now = time.time()
            with self._lock:
                expired = [
                    ip
                    for ip, info in self._peers.items()
                    if (now - info["last_seen"]) >= PEER_TTL and ip != self.node_ip
                ]
                for ip in expired:
                    self.logger.info(
                        "Peer expired: %s @ %s", self._peers[ip]["node_id"], ip
                    )
                    del self._peers[ip]
