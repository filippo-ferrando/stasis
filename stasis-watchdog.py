#!/usr/bin/env python3
"""
Watchdog Filesystem Event Monitor

This module monitors a filesystem directory for changes to .qcow2 files
and sends events to the blockchain service for recording.

Features:
- Monitors filesystem events (create, modify, delete, move)
- Computes BLAKE3 hashes of file contents
- Uses sampled fingerprinting for very large files to bound hashing time
- Filters for .qcow2 files only
- Sends events to blockchain via HTTP API

Environment Variables:
    BLOCKCHAIN_API:          URL of blockchain service (default: http://blockchain:5000/event)
    WATCH_PATH:              Directory to monitor (default: /images)
    HASH_FULL_THRESHOLD_MB:  Files smaller than this are fully hashed (default: 512)
    HASH_SAMPLE_COUNT:       Number of chunks sampled for large files (default: 64)
    HASH_SAMPLE_SIZE_MB:     Size of each sample chunk in MB (default: 4)

Usage:
    python stasis-watchdog.py
"""

import os
import time
import blake3
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

BLOCKCHAIN_API = os.environ.get("BLOCKCHAIN_API", "http://blockchain:5000/event")
WATCH_PATH = os.environ.get("WATCH_PATH", "/images")

# Hashing configuration
HASH_FULL_THRESHOLD_MB = int(os.environ.get("HASH_FULL_THRESHOLD_MB", "512"))
HASH_SAMPLE_COUNT = int(os.environ.get("HASH_SAMPLE_COUNT", "64"))
HASH_SAMPLE_SIZE_MB = int(os.environ.get("HASH_SAMPLE_SIZE_MB", "4"))

_MB = 1024 * 1024
HASH_FULL_THRESHOLD = HASH_FULL_THRESHOLD_MB * _MB
HASH_SAMPLE_SIZE = HASH_SAMPLE_SIZE_MB * _MB


def compute_blake3_full(path: str) -> str:
    """
    Compute BLAKE3 hash of an entire file using streaming reads.

    Args:
        path: Path to file

    Returns:
        Hex-encoded BLAKE3 digest
    """
    h = blake3.blake3()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(HASH_SAMPLE_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compute_blake3_sampled(path: str, file_size: int) -> str:
    """
    Compute a BLAKE3-based sampled fingerprint for large files.

    Reads HASH_SAMPLE_COUNT evenly-spaced chunks of HASH_SAMPLE_SIZE bytes
    and includes the file size in the hash to detect truncation.  This bounds
    hashing time regardless of file size while still detecting most mutations.

    Args:
        path:      Path to file
        file_size: Known file size in bytes (from stat)

    Returns:
        Hex-encoded BLAKE3 digest prefixed with "sampled:"
    """
    h = blake3.blake3()
    h.update(file_size.to_bytes(8, "big"))  # include size in digest

    with open(path, "rb") as f:
        step = max(file_size // HASH_SAMPLE_COUNT, HASH_SAMPLE_SIZE)
        for i in range(HASH_SAMPLE_COUNT):
            offset = i * step
            if offset >= file_size:
                break
            f.seek(offset)
            chunk = f.read(HASH_SAMPLE_SIZE)
            if chunk:
                h.update(chunk)

    return "sampled:" + h.hexdigest()


def compute_content_hash(path: str, size_bytes: int) -> str:
    """
    Select full or sampled BLAKE3 hashing based on file size.

    Args:
        path:       Path to file
        size_bytes: File size in bytes

    Returns:
        Hex-encoded BLAKE3 digest (optionally prefixed with "sampled:")
    """
    if size_bytes >= HASH_FULL_THRESHOLD:
        return compute_blake3_sampled(path, size_bytes)
    return compute_blake3_full(path)


class ImageEventHandler(FileSystemEventHandler):
    """
    Filesystem event handler for .qcow2 image files.

    Monitors filesystem changes and sends events to blockchain service.
    Only processes .qcow2 files, ignores directories and other file types.
    """

    def dispatch(self, event):
        """Override dispatch to filter out directory events."""
        if event.is_directory:
            return
        super().dispatch(event)

    def on_created(self, event):
        """Handle file creation event."""
        self.process(event, "create")

    def on_modified(self, event):
        """Handle file modification event."""
        self.process(event, "modify")

    def on_deleted(self, event):
        """Handle file deletion event."""
        self.process(event, "delete")

    def on_moved(self, event):
        """Handle file move/rename event."""
        self.process(event, "move", dest_path=event.dest_path)

    def process(self, event, event_type, dest_path=None):
        """
        Process a filesystem event and send to blockchain.

        Extracts file metadata (inode, size, BLAKE3 hash) and sends to blockchain
        service via HTTP POST.  Only processes .qcow2 files.

        Args:
            event:      Watchdog event object
            event_type: Type of event (create, modify, delete, move)
            dest_path:  Destination path for move events
        """
        filepath = event.src_path
        if not filepath.endswith(".qcow2"):
            return

        metadata = {}
        size_bytes = 0
        inode = 0
        content_hash = ""

        if event_type != "delete":
            try:
                stat = os.stat(filepath)
                inode = stat.st_ino
                size_bytes = stat.st_size
                content_hash = compute_content_hash(filepath, size_bytes)
            except FileNotFoundError:
                # May happen on rapid delete/move
                return

        payload = {
            "event": event_type,
            "path": filepath,
            "inode": inode,
            "size_bytes": size_bytes,
            "content_hash": content_hash,
            "metadata": metadata,
            "dest_path": dest_path,
        }

        try:
            requests.post(BLOCKCHAIN_API, json=payload, timeout=5)
            print(f"[WATCHDOG] action performed {payload}")
        except Exception as e:
            print(f"Failed to send event to blockchain: {e}")


if __name__ == "__main__":
    print(f"[Watchdog] Watching: {WATCH_PATH}")
    print(
        f"[Watchdog] Hash threshold: {HASH_FULL_THRESHOLD_MB} MB "
        f"(sampled={HASH_SAMPLE_COUNT}×{HASH_SAMPLE_SIZE_MB} MB above threshold)"
    )
    event_handler = ImageEventHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_PATH, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

