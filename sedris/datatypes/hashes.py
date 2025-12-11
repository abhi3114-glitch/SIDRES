"""
Hash data type handler for Sedris.

Implements Redis hash commands: HSET, HGET, HDEL, HGETALL, etc.
"""

from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..store import DataStore

from ..store import DataType


class HashHandler:
    """Handler for hash operations."""
    
    def __init__(self, store: "DataStore"):
        self.store = store
    
    def _get_hash(self, key: str, create: bool = False) -> Optional[Dict[str, str]]:
        """Get hash value, optionally creating if not exists."""
        if not self.store.check_type(key, DataType.HASH):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        if create:
            return self.store.get_or_create(key, DataType.HASH, dict)
        return self.store.get(key)
    
    def hset(self, key: str, *args) -> int:
        """Set hash fields. Args: field1, val1, field2, val2, ..."""
        if len(args) % 2 != 0:
            raise ValueError("ERR wrong number of arguments for HSET")
        
        h = self._get_hash(key, create=True)
        new_fields = 0
        
        for i in range(0, len(args), 2):
            field, value = args[i], args[i + 1]
            if field not in h:
                new_fields += 1
            h[field] = value
        
        return new_fields
    
    def hsetnx(self, key: str, field: str, value: str) -> int:
        """Set field only if it doesn't exist."""
        h = self._get_hash(key, create=True)
        if field in h:
            return 0
        h[field] = value
        return 1
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """Get value of field."""
        h = self._get_hash(key)
        if not h:
            return None
        return h.get(field)
    
    def hmset(self, key: str, *args) -> str:
        """Set multiple fields (deprecated but supported)."""
        self.hset(key, *args)
        return "OK"
    
    def hmget(self, key: str, *fields: str) -> List[Optional[str]]:
        """Get values of multiple fields."""
        h = self._get_hash(key) or {}
        return [h.get(f) for f in fields]
    
    def hdel(self, key: str, *fields: str) -> int:
        """Delete fields. Returns count of deleted."""
        h = self._get_hash(key)
        if not h:
            return 0
        
        count = 0
        for field in fields:
            if field in h:
                del h[field]
                count += 1
        
        if not h:
            self.store.delete(key)
        
        return count
    
    def hexists(self, key: str, field: str) -> int:
        """Check if field exists."""
        h = self._get_hash(key)
        return 1 if h and field in h else 0
    
    def hlen(self, key: str) -> int:
        """Get number of fields."""
        h = self._get_hash(key)
        return len(h) if h else 0
    
    def hkeys(self, key: str) -> List[str]:
        """Get all field names."""
        h = self._get_hash(key)
        return list(h.keys()) if h else []
    
    def hvals(self, key: str) -> List[str]:
        """Get all values."""
        h = self._get_hash(key)
        return list(h.values()) if h else []
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all fields and values."""
        h = self._get_hash(key)
        return dict(h) if h else {}
    
    def hincrby(self, key: str, field: str, increment: int) -> int:
        """Increment field by integer amount."""
        h = self._get_hash(key, create=True)
        
        current = h.get(field, "0")
        try:
            value = int(current)
        except ValueError:
            raise ValueError("ERR hash value is not an integer")
        
        new_value = value + increment
        h[field] = str(new_value)
        return new_value
    
    def hincrbyfloat(self, key: str, field: str, increment: float) -> str:
        """Increment field by float amount."""
        h = self._get_hash(key, create=True)
        
        current = h.get(field, "0")
        try:
            value = float(current)
        except ValueError:
            raise ValueError("ERR hash value is not a float")
        
        new_value = value + increment
        result = f"{new_value:.17g}"
        h[field] = result
        return result
    
    def hstrlen(self, key: str, field: str) -> int:
        """Get string length of field value."""
        h = self._get_hash(key)
        if not h or field not in h:
            return 0
        return len(h[field])
    
    def hrandfield(self, key: str, count: int = None, withvalues: bool = False):
        """Get random field(s)."""
        import random
        
        h = self._get_hash(key)
        if not h:
            return None if count is None else []
        
        fields = list(h.keys())
        
        if count is None:
            return random.choice(fields)
        
        if count >= 0:
            count = min(count, len(fields))
            selected = random.sample(fields, count)
        else:
            selected = [random.choice(fields) for _ in range(abs(count))]
        
        if withvalues:
            result = []
            for f in selected:
                result.extend([f, h[f]])
            return result
        
        return selected
    
    def hscan(self, key: str, cursor: int, match: str = "*", count: int = 10) -> tuple:
        """Incrementally iterate hash fields."""
        import fnmatch
        
        h = self._get_hash(key)
        if not h:
            return (0, [])
        
        fields = list(h.keys())
        
        # Filter by pattern
        if match != "*":
            fields = [f for f in fields if fnmatch.fnmatch(f, match)]
        
        # Create pairs
        pairs = []
        for f in fields:
            pairs.extend([f, h[f]])
        
        # Paginate (by field count, not pair count)
        start = cursor
        end = min(start + count, len(fields))
        
        if end >= len(fields):
            next_cursor = 0
        else:
            next_cursor = end
        
        result_pairs = []
        for f in fields[start:end]:
            result_pairs.extend([f, h[f]])
        
        return (next_cursor, result_pairs)
