"""
Core in-memory data store for Sedris.

Implements thread-safe key-value storage with TTL support.
"""

import threading
import time
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple


class DataType:
    """Constants for data types."""
    STRING = "string"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    NONE = "none"


class DataStore:
    """
    In-memory data store with TTL support.
    
    Thread-safe implementation using locks for concurrent access.
    """
    
    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._types: Dict[str, str] = {}
        self._expires: Dict[str, float] = {}  # key -> expiration timestamp
        self._lock = threading.RLock()
        self._dirty_count = 0  # Changes since last save
        self._last_save = time.time()
    
    # ==================== Core Operations ====================
    
    def get(self, key: str) -> Optional[Any]:
        """Get value by key. Returns None if expired or not found."""
        with self._lock:
            if self._is_expired(key):
                self._delete_key(key)
                return None
            return self._data.get(key)
    
    def set(self, key: str, value: Any, dtype: str = DataType.STRING) -> None:
        """Set a key-value pair with optional type."""
        with self._lock:
            self._data[key] = value
            self._types[key] = dtype
            self._dirty_count += 1
    
    def delete(self, *keys: str) -> int:
        """Delete one or more keys. Returns count of deleted keys."""
        with self._lock:
            count = 0
            for key in keys:
                if key in self._data:
                    self._delete_key(key)
                    count += 1
            return count
    
    def exists(self, *keys: str) -> int:
        """Check if keys exist. Returns count of existing keys."""
        with self._lock:
            count = 0
            for key in keys:
                if self._is_expired(key):
                    self._delete_key(key)
                elif key in self._data:
                    count += 1
            return count
    
    def keys(self, pattern: str = "*") -> List[str]:
        """Get all keys matching pattern. Supports * and ? wildcards."""
        import fnmatch
        with self._lock:
            self._cleanup_expired()
            if pattern == "*":
                return list(self._data.keys())
            return [k for k in self._data.keys() if fnmatch.fnmatch(k, pattern)]
    
    def type(self, key: str) -> str:
        """Get the type of a key."""
        with self._lock:
            if self._is_expired(key):
                self._delete_key(key)
                return DataType.NONE
            return self._types.get(key, DataType.NONE)
    
    def rename(self, old_key: str, new_key: str) -> bool:
        """Rename a key."""
        with self._lock:
            if old_key not in self._data:
                return False
            if self._is_expired(old_key):
                self._delete_key(old_key)
                return False
            
            self._data[new_key] = self._data.pop(old_key)
            self._types[new_key] = self._types.pop(old_key, DataType.STRING)
            if old_key in self._expires:
                self._expires[new_key] = self._expires.pop(old_key)
            self._dirty_count += 1
            return True
    
    def dbsize(self) -> int:
        """Return the number of keys in the store."""
        with self._lock:
            self._cleanup_expired()
            return len(self._data)
    
    def flushdb(self) -> None:
        """Delete all keys in the current database."""
        with self._lock:
            self._data.clear()
            self._types.clear()
            self._expires.clear()
            self._dirty_count += 1
    
    def randomkey(self) -> Optional[str]:
        """Return a random key."""
        import random
        with self._lock:
            self._cleanup_expired()
            if not self._data:
                return None
            return random.choice(list(self._data.keys()))
    
    # ==================== TTL Operations ====================
    
    def expire(self, key: str, seconds: int) -> bool:
        """Set TTL on a key in seconds."""
        with self._lock:
            if key not in self._data:
                return False
            if self._is_expired(key):
                self._delete_key(key)
                return False
            self._expires[key] = time.time() + seconds
            return True
    
    def expireat(self, key: str, timestamp: float) -> bool:
        """Set expiration as Unix timestamp."""
        with self._lock:
            if key not in self._data:
                return False
            if self._is_expired(key):
                self._delete_key(key)
                return False
            self._expires[key] = timestamp
            return True
    
    def ttl(self, key: str) -> int:
        """Get TTL in seconds. Returns -2 if not found, -1 if no expiry."""
        with self._lock:
            if key not in self._data:
                return -2
            if self._is_expired(key):
                self._delete_key(key)
                return -2
            if key not in self._expires:
                return -1
            remaining = self._expires[key] - time.time()
            return max(0, int(remaining))
    
    def pttl(self, key: str) -> int:
        """Get TTL in milliseconds."""
        with self._lock:
            if key not in self._data:
                return -2
            if self._is_expired(key):
                self._delete_key(key)
                return -2
            if key not in self._expires:
                return -1
            remaining = (self._expires[key] - time.time()) * 1000
            return max(0, int(remaining))
    
    def persist(self, key: str) -> bool:
        """Remove expiration from a key."""
        with self._lock:
            if key not in self._data:
                return False
            if key in self._expires:
                del self._expires[key]
                return True
            return False
    
    # ==================== Type Checking ====================
    
    def check_type(self, key: str, expected: str) -> bool:
        """Check if key is of expected type or doesn't exist."""
        with self._lock:
            if key not in self._data:
                return True
            if self._is_expired(key):
                self._delete_key(key)
                return True
            return self._types.get(key) == expected
    
    def get_or_create(self, key: str, dtype: str, default_factory) -> Any:
        """Get existing value or create new one with default."""
        with self._lock:
            if self._is_expired(key):
                self._delete_key(key)
            
            if key not in self._data:
                self._data[key] = default_factory()
                self._types[key] = dtype
                self._dirty_count += 1
            
            return self._data[key]
    
    # ==================== Internal Helpers ====================
    
    def _is_expired(self, key: str) -> bool:
        """Check if a key has expired."""
        if key not in self._expires:
            return False
        return time.time() > self._expires[key]
    
    def _delete_key(self, key: str) -> None:
        """Delete a key and its metadata."""
        self._data.pop(key, None)
        self._types.pop(key, None)
        self._expires.pop(key, None)
        self._dirty_count += 1
    
    def _cleanup_expired(self) -> int:
        """Remove all expired keys. Returns count removed."""
        count = 0
        expired_keys = [k for k in self._expires if self._is_expired(k)]
        for key in expired_keys:
            self._delete_key(key)
            count += 1
        return count
    
    # ==================== Persistence Helpers ====================
    
    def get_snapshot(self) -> Tuple[Dict, Dict, Dict]:
        """Get a snapshot of current state for persistence."""
        with self._lock:
            self._cleanup_expired()
            return (
                dict(self._data),
                dict(self._types),
                dict(self._expires)
            )
    
    def restore_snapshot(self, data: Dict, types: Dict, expires: Dict) -> None:
        """Restore state from a snapshot."""
        with self._lock:
            self._data = data
            self._types = types
            self._expires = expires
            self._cleanup_expired()
    
    @property
    def dirty_count(self) -> int:
        """Number of changes since last save."""
        return self._dirty_count
    
    def reset_dirty_count(self) -> None:
        """Reset the dirty counter after save."""
        with self._lock:
            self._dirty_count = 0
            self._last_save = time.time()
    
    @property
    def last_save(self) -> float:
        """Timestamp of last save."""
        return self._last_save
