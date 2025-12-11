"""
Set data type handler for Sedris.

Implements Redis set commands: SADD, SREM, SMEMBERS, SINTER, SUNION, etc.
"""

import random
from typing import List, Optional, Set as PySet, TYPE_CHECKING

if TYPE_CHECKING:
    from ..store import DataStore

from ..store import DataType


class SetHandler:
    """Handler for set operations."""
    
    def __init__(self, store: "DataStore"):
        self.store = store
    
    def _get_set(self, key: str, create: bool = False) -> Optional[PySet[str]]:
        """Get set value, optionally creating if not exists."""
        if not self.store.check_type(key, DataType.SET):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        if create:
            return self.store.get_or_create(key, DataType.SET, set)
        return self.store.get(key)
    
    def sadd(self, key: str, *members: str) -> int:
        """Add members to set. Returns count of new members."""
        s = self._get_set(key, create=True)
        initial_size = len(s)
        s.update(members)
        return len(s) - initial_size
    
    def srem(self, key: str, *members: str) -> int:
        """Remove members from set. Returns count of removed."""
        s = self._get_set(key)
        if not s:
            return 0
        
        count = 0
        for member in members:
            if member in s:
                s.remove(member)
                count += 1
        
        if not s:
            self.store.delete(key)
        
        return count
    
    def sismember(self, key: str, member: str) -> int:
        """Check if member exists. Returns 1 if exists, 0 otherwise."""
        s = self._get_set(key)
        return 1 if s and member in s else 0
    
    def smismember(self, key: str, *members: str) -> List[int]:
        """Check multiple members. Returns list of 0/1."""
        s = self._get_set(key) or set()
        return [1 if m in s else 0 for m in members]
    
    def smembers(self, key: str) -> List[str]:
        """Get all members."""
        s = self._get_set(key)
        return list(s) if s else []
    
    def scard(self, key: str) -> int:
        """Get set cardinality (size)."""
        s = self._get_set(key)
        return len(s) if s else 0
    
    def spop(self, key: str, count: int = 1) -> Optional[any]:
        """Remove and return random member(s)."""
        s = self._get_set(key)
        if not s:
            return None
        
        if count == 1:
            member = random.choice(list(s))
            s.remove(member)
            if not s:
                self.store.delete(key)
            return member
        
        count = min(count, len(s))
        members = random.sample(list(s), count)
        for m in members:
            s.remove(m)
        
        if not s:
            self.store.delete(key)
        
        return members
    
    def srandmember(self, key: str, count: int = None) -> Optional[any]:
        """Get random member(s) without removing."""
        s = self._get_set(key)
        if not s:
            return None if count is None else []
        
        if count is None:
            return random.choice(list(s))
        
        if count >= 0:
            count = min(count, len(s))
            return random.sample(list(s), count)
        else:
            # Negative count: allow duplicates
            return [random.choice(list(s)) for _ in range(abs(count))]
    
    def smove(self, source: str, destination: str, member: str) -> int:
        """Move member from source to destination."""
        src = self._get_set(source)
        if not src or member not in src:
            return 0
        
        dst = self._get_set(destination, create=True)
        src.remove(member)
        dst.add(member)
        
        if not src:
            self.store.delete(source)
        
        return 1
    
    def sunion(self, *keys: str) -> List[str]:
        """Return union of sets."""
        result = set()
        for key in keys:
            s = self._get_set(key)
            if s:
                result.update(s)
        return list(result)
    
    def sunionstore(self, destination: str, *keys: str) -> int:
        """Store union and return size."""
        result = set(self.sunion(*keys))
        if result:
            self.store.set(destination, result, DataType.SET)
        else:
            self.store.delete(destination)
        return len(result)
    
    def sinter(self, *keys: str) -> List[str]:
        """Return intersection of sets."""
        sets = []
        for key in keys:
            s = self._get_set(key)
            if s is None:
                return []  # Empty intersection if any key missing
            sets.append(s)
        
        if not sets:
            return []
        
        result = sets[0].copy()
        for s in sets[1:]:
            result &= s
        return list(result)
    
    def sinterstore(self, destination: str, *keys: str) -> int:
        """Store intersection and return size."""
        result = set(self.sinter(*keys))
        if result:
            self.store.set(destination, result, DataType.SET)
        else:
            self.store.delete(destination)
        return len(result)
    
    def sintercard(self, numkeys: int, *keys: str, limit: int = 0) -> int:
        """Return cardinality of intersection."""
        keys = keys[:numkeys]
        result = self.sinter(*keys)
        if limit > 0:
            return min(len(result), limit)
        return len(result)
    
    def sdiff(self, *keys: str) -> List[str]:
        """Return difference (first set minus others)."""
        if not keys:
            return []
        
        base = self._get_set(keys[0])
        if not base:
            return []
        
        result = base.copy()
        for key in keys[1:]:
            s = self._get_set(key)
            if s:
                result -= s
        
        return list(result)
    
    def sdiffstore(self, destination: str, *keys: str) -> int:
        """Store difference and return size."""
        result = set(self.sdiff(*keys))
        if result:
            self.store.set(destination, result, DataType.SET)
        else:
            self.store.delete(destination)
        return len(result)
    
    def sscan(self, key: str, cursor: int, match: str = "*", count: int = 10) -> tuple:
        """Incrementally iterate set members."""
        import fnmatch
        
        s = self._get_set(key)
        if not s:
            return (0, [])
        
        members = list(s)
        
        # Filter by pattern
        if match != "*":
            members = [m for m in members if fnmatch.fnmatch(m, match)]
        
        # Paginate
        start = cursor
        end = min(start + count, len(members))
        
        if end >= len(members):
            next_cursor = 0
        else:
            next_cursor = end
        
        return (next_cursor, members[start:end])
