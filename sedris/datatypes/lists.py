"""
List data type handler for Sedris.

Implements Redis list commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.
Uses Python's deque for O(1) push/pop on both ends.
"""

from collections import deque
from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..store import DataStore

from ..store import DataType


class ListHandler:
    """Handler for list operations."""
    
    def __init__(self, store: "DataStore"):
        self.store = store
    
    def _get_list(self, key: str, create: bool = False) -> Optional[deque]:
        """Get list value, optionally creating if not exists."""
        if not self.store.check_type(key, DataType.LIST):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        if create:
            return self.store.get_or_create(key, DataType.LIST, deque)
        return self.store.get(key)
    
    def lpush(self, key: str, *values: str) -> int:
        """Insert values at head. Returns new length."""
        lst = self._get_list(key, create=True)
        for value in values:
            lst.appendleft(value)
        return len(lst)
    
    def lpushx(self, key: str, *values: str) -> int:
        """Insert at head only if list exists."""
        lst = self._get_list(key)
        if lst is None:
            return 0
        for value in values:
            lst.appendleft(value)
        return len(lst)
    
    def rpush(self, key: str, *values: str) -> int:
        """Insert values at tail. Returns new length."""
        lst = self._get_list(key, create=True)
        for value in values:
            lst.append(value)
        return len(lst)
    
    def rpushx(self, key: str, *values: str) -> int:
        """Insert at tail only if list exists."""
        lst = self._get_list(key)
        if lst is None:
            return 0
        for value in values:
            lst.append(value)
        return len(lst)
    
    def lpop(self, key: str, count: int = 1) -> Optional[Any]:
        """Remove and return head element(s)."""
        lst = self._get_list(key)
        if not lst:
            return None
        
        if count == 1:
            result = lst.popleft()
        else:
            result = []
            for _ in range(min(count, len(lst))):
                result.append(lst.popleft())
        
        # Delete key if list is empty
        if not lst:
            self.store.delete(key)
        
        return result
    
    def rpop(self, key: str, count: int = 1) -> Optional[Any]:
        """Remove and return tail element(s)."""
        lst = self._get_list(key)
        if not lst:
            return None
        
        if count == 1:
            result = lst.pop()
        else:
            result = []
            for _ in range(min(count, len(lst))):
                result.append(lst.pop())
        
        if not lst:
            self.store.delete(key)
        
        return result
    
    def llen(self, key: str) -> int:
        """Get list length."""
        lst = self._get_list(key)
        return len(lst) if lst else 0
    
    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """Get range of elements. Negative indices count from end."""
        lst = self._get_list(key)
        if not lst:
            return []
        
        length = len(lst)
        
        # Handle negative indices
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        
        # Redis includes stop index
        stop = min(stop + 1, length)
        
        if start >= length or start > stop:
            return []
        
        return list(lst)[start:stop]
    
    def lindex(self, key: str, index: int) -> Optional[str]:
        """Get element at index."""
        lst = self._get_list(key)
        if not lst:
            return None
        
        length = len(lst)
        if index < 0:
            index = length + index
        
        if 0 <= index < length:
            return list(lst)[index]
        return None
    
    def lset(self, key: str, index: int, value: str) -> str:
        """Set element at index."""
        lst = self._get_list(key)
        if not lst:
            raise IndexError("ERR no such key")
        
        length = len(lst)
        if index < 0:
            index = length + index
        
        if not (0 <= index < length):
            raise IndexError("ERR index out of range")
        
        # Convert to list, modify, convert back
        items = list(lst)
        items[index] = value
        lst.clear()
        lst.extend(items)
        return "OK"
    
    def linsert(self, key: str, position: str, pivot: str, value: str) -> int:
        """Insert before/after pivot element."""
        lst = self._get_list(key)
        if not lst:
            return 0
        
        items = list(lst)
        try:
            pivot_idx = items.index(pivot)
        except ValueError:
            return -1
        
        if position.upper() == "BEFORE":
            items.insert(pivot_idx, value)
        else:  # AFTER
            items.insert(pivot_idx + 1, value)
        
        lst.clear()
        lst.extend(items)
        return len(lst)
    
    def lrem(self, key: str, count: int, value: str) -> int:
        """Remove count occurrences of value."""
        lst = self._get_list(key)
        if not lst:
            return 0
        
        items = list(lst)
        removed = 0
        
        if count > 0:
            # Remove from head
            new_items = []
            for item in items:
                if item == value and removed < count:
                    removed += 1
                else:
                    new_items.append(item)
            items = new_items
        elif count < 0:
            # Remove from tail
            count = abs(count)
            new_items = []
            for item in reversed(items):
                if item == value and removed < count:
                    removed += 1
                else:
                    new_items.append(item)
            items = list(reversed(new_items))
        else:
            # Remove all
            items = [item for item in items if item != value]
            removed = len(lst) - len(items)
        
        if items:
            lst.clear()
            lst.extend(items)
        else:
            self.store.delete(key)
        
        return removed
    
    def ltrim(self, key: str, start: int, stop: int) -> str:
        """Trim list to specified range."""
        lst = self._get_list(key)
        if not lst:
            return "OK"
        
        length = len(lst)
        
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        
        stop = min(stop + 1, length)
        
        if start >= length or start > stop:
            self.store.delete(key)
        else:
            items = list(lst)[start:stop]
            lst.clear()
            lst.extend(items)
        
        return "OK"
    
    def rpoplpush(self, source: str, destination: str) -> Optional[str]:
        """Pop from source tail, push to destination head."""
        value = self.rpop(source, 1)
        if value is None:
            return None
        
        # rpop returns single value when count=1
        self.lpush(destination, value)
        return value
    
    def lmove(self, source: str, dest: str, wherefrom: str, whereto: str) -> Optional[str]:
        """Move element between lists."""
        if wherefrom.upper() == "LEFT":
            value = self.lpop(source, 1)
        else:
            value = self.rpop(source, 1)
        
        if value is None:
            return None
        
        if whereto.upper() == "LEFT":
            self.lpush(dest, value)
        else:
            self.rpush(dest, value)
        
        return value
    
    def lpos(self, key: str, element: str, rank: int = 1, count: int = 1, maxlen: int = 0) -> Any:
        """Find position(s) of element."""
        lst = self._get_list(key)
        if not lst:
            return None
        
        items = list(lst)
        if maxlen > 0:
            items = items[:maxlen]
        
        positions = [i for i, v in enumerate(items) if v == element]
        
        if rank < 0:
            positions = list(reversed(positions))
            rank = abs(rank)
        
        # Skip first (rank-1) matches
        positions = positions[rank - 1:]
        
        if count == 1:
            return positions[0] if positions else None
        
        return positions[:count] if count > 0 else positions
