"""
Persistence module for Sedris.

Implements RDB-style persistence with background saving.
"""

import json
import os
import pickle
import threading
import time
import logging
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .store import DataStore
    from .config import Config

logger = logging.getLogger("sedris")


class Persistence:
    """Handles saving and loading data store state."""
    
    def __init__(self, store: "DataStore", config: "Config"):
        self.store = store
        self.config = config
        self._save_thread: Optional[threading.Thread] = None
        self._running = False
        self._last_save_time = time.time()
        self._last_save_changes = 0
    
    def start_background_save(self):
        """Start background save thread."""
        self._running = True
        self._save_thread = threading.Thread(target=self._save_loop, daemon=True)
        self._save_thread.start()
        logger.info("Background save thread started")
    
    def stop(self):
        """Stop background save thread."""
        self._running = False
        if self._save_thread:
            self._save_thread.join(timeout=5)
    
    def _save_loop(self):
        """Background save loop."""
        while self._running:
            time.sleep(1)  # Check every second
            
            now = time.time()
            changes = self.store.dirty_count - self._last_save_changes
            
            # Check save intervals
            for seconds, min_changes in self.config.save_intervals:
                elapsed = now - self._last_save_time
                if elapsed >= seconds and changes >= min_changes:
                    self.save()
                    break
    
    def save(self) -> bool:
        """Save current state to disk."""
        try:
            # Ensure data directory exists
            os.makedirs(self.config.data_dir, exist_ok=True)
            
            # Get snapshot
            data, types, expires = self.store.get_snapshot()
            
            # Prepare data for serialization
            save_data = {
                "version": 1,
                "timestamp": time.time(),
                "data": self._serialize_data(data, types),
                "expires": {k: v for k, v in expires.items()}
            }
            
            # Write to temp file first, then rename (atomic)
            temp_path = self.config.rdb_path + ".tmp"
            with open(temp_path, 'wb') as f:
                pickle.dump(save_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Rename to final path
            if os.path.exists(self.config.rdb_path):
                os.remove(self.config.rdb_path)
            os.rename(temp_path, self.config.rdb_path)
            
            # Update tracking
            self._last_save_time = time.time()
            self._last_save_changes = self.store.dirty_count
            self.store.reset_dirty_count()
            
            logger.info(f"Saved to {self.config.rdb_path}")
            return True
        
        except Exception as e:
            logger.error(f"Save failed: {e}")
            return False
    
    def load(self) -> bool:
        """Load state from disk."""
        if not os.path.exists(self.config.rdb_path):
            logger.info("No RDB file found, starting with empty database")
            return False
        
        try:
            with open(self.config.rdb_path, 'rb') as f:
                save_data = pickle.load(f)
            
            version = save_data.get("version", 1)
            if version != 1:
                logger.warning(f"Unknown RDB version {version}")
                return False
            
            # Restore data
            data, types = self._deserialize_data(save_data["data"])
            expires = save_data.get("expires", {})
            
            self.store.restore_snapshot(data, types, expires)
            
            logger.info(f"Loaded {len(data)} keys from {self.config.rdb_path}")
            return True
        
        except Exception as e:
            logger.error(f"Load failed: {e}")
            return False
    
    def _serialize_data(self, data: dict, types: dict) -> list:
        """Serialize data for storage."""
        result = []
        for key, value in data.items():
            dtype = types.get(key, "string")
            result.append({
                "key": key,
                "type": dtype,
                "value": self._serialize_value(value, dtype)
            })
        return result
    
    def _serialize_value(self, value, dtype: str):
        """Serialize a value based on its type."""
        if dtype == "string":
            return value
        elif dtype == "list":
            return list(value)
        elif dtype == "set":
            return list(value)
        elif dtype == "hash":
            return dict(value)
        elif dtype == "zset":
            # Sorted set - serialize member scores and sorted list
            return {
                "member_scores": value.member_scores,
                "score_members": value.score_members
            }
        return value
    
    def _deserialize_data(self, saved: list) -> tuple:
        """Deserialize data from storage."""
        from collections import deque
        from .datatypes.sorted_sets import SortedSet
        
        data = {}
        types = {}
        
        for item in saved:
            key = item["key"]
            dtype = item["type"]
            value = item["value"]
            
            types[key] = dtype
            
            if dtype == "string":
                data[key] = value
            elif dtype == "list":
                data[key] = deque(value)
            elif dtype == "set":
                data[key] = set(value)
            elif dtype == "hash":
                data[key] = dict(value)
            elif dtype == "zset":
                zset = SortedSet()
                zset.member_scores = value["member_scores"]
                zset.score_members = [tuple(x) for x in value["score_members"]]
                data[key] = zset
            else:
                data[key] = value
        
        return data, types


class AOFPersistence:
    """
    Append-Only File persistence (simplified implementation).
    
    Logs each write command for replay on restart.
    """
    
    def __init__(self, config: "Config"):
        self.config = config
        self.aof_path = os.path.join(config.data_dir, "appendonly.aof")
        self._file = None
    
    def open(self):
        """Open AOF file for appending."""
        os.makedirs(self.config.data_dir, exist_ok=True)
        self._file = open(self.aof_path, 'ab')
    
    def close(self):
        """Close AOF file."""
        if self._file:
            self._file.close()
            self._file = None
    
    def log_command(self, command: list):
        """Log a command to AOF."""
        if not self._file:
            return
        
        # Write in RESP format
        line = f"*{len(command)}\r\n"
        for arg in command:
            arg_bytes = str(arg).encode('utf-8')
            line += f"${len(arg_bytes)}\r\n{arg}\r\n"
        
        self._file.write(line.encode('utf-8'))
        self._file.flush()
    
    def replay(self, command_handler) -> int:
        """Replay AOF file commands."""
        if not os.path.exists(self.aof_path):
            return 0
        
        from .protocol import RESPParser
        
        count = 0
        parser = RESPParser()
        
        with open(self.aof_path, 'rb') as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                
                parser.feed(data)
                
                while True:
                    command = parser.get_message()
                    if command is None:
                        break
                    
                    command_handler.execute(command)
                    count += 1
        
        return count
