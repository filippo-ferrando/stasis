#!/usr/bin/env python3
"""
Watchdog Filesystem Event Monitor

This module monitors a filesystem directory for changes to .qcow2 files
and sends events to the blockchain service for recording.

Features:
- Monitors filesystem events (create, modify, delete, move)
- Computes SHA256 hashes of file contents
- Filters for .qcow2 files only
- Sends events to blockchain via HTTP API

Environment Variables:
    BLOCKCHAIN_API: URL of blockchain service (default: http://blockchain:5000/event)
    WATCH_PATH: Directory to monitor (default: /images)

Usage:
    python watchdog-images.py
"""
import os
import time
import json
import hashlib
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

BLOCKCHAIN_API = os.environ.get("BLOCKCHAIN_API", "http://blockchain:5000/event")
WATCH_PATH = os.environ.get("WATCH_PATH", "/images")
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB


def compute_sha256(path):
    """
    Compute streaming SHA256 hash of a file.
    
    Uses chunked reading to handle large files efficiently
    without loading entire file into memory.
    
    Args:
        path (str): Path to file
        
    Returns:
        str: Hexadecimal SHA256 hash of file contents
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


class ImageEventHandler(FileSystemEventHandler):
    """
    Filesystem event handler for .qcow2 image files.
    
    Monitors filesystem changes and sends events to blockchain service.
    Only processes .qcow2 files, ignores directories and other file types.
    """
    
    def dispatch(self, event):
        """
        Override dispatch to filter out directory events.
        
        Args:
            event: Watchdog event object
        """
        # Only handle files, ignore directories
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
        
        Extracts file metadata (inode, size, hash) and sends to blockchain
        service via HTTP POST. Only processes .qcow2 files.
        
        Args:
            event: Watchdog event object
            event_type (str): Type of event (create, modify, delete, move)
            dest_path (str, optional): Destination path for move events
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
                content_hash = compute_sha256(filepath)
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
