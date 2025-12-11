"""
String data type handler for Sedris.

Implements Redis string commands: GET, SET, APPEND, STRLEN, INCR, DECR, etc.
"""

from typing import Any, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ..store import DataStore

from ..store import DataType


class StringHandler:
    """Handler for string operations."""
    
    def __init__(self, store: "DataStore"):
        self.store = store
    
    def get(self, key: str) -> Optional[str]:
        """Get the value of a key."""
        if not self.store.check_type(key, DataType.STRING):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        value = self.store.get(key)
        return str(value) if value is not None else None
    
    def set(self, key: str, value: str, ex: int = None, px: int = None,
            nx: bool = False, xx: bool = False, keepttl: bool = False) -> Optional[str]:
        """
        Set a key to a value.
        
        Args:
            key: Key name
            value: Value to set
            ex: Expire in seconds
            px: Expire in milliseconds
            nx: Only set if key doesn't exist
            xx: Only set if key exists
            keepttl: Keep existing TTL
        
        Returns:
            "OK" on success, None if condition not met
        """
        # Check NX/XX conditions
        exists = self.store.exists(key) > 0
        if nx and exists:
            return None
        if xx and not exists:
            return None
        
        # Get current TTL if keepttl
        current_ttl = None
        if keepttl and exists:
            current_ttl = self.store.pttl(key)
            if current_ttl < 0:
                current_ttl = None
        
        # Set the value
        self.store.set(key, value, DataType.STRING)
        
        # Set expiration
        if ex is not None:
            self.store.expire(key, ex)
        elif px is not None:
            self.store.expire(key, px // 1000)  # Convert to seconds
        elif keepttl and current_ttl is not None:
            self.store.expire(key, current_ttl // 1000)
        
        return "OK"
    
    def setnx(self, key: str, value: str) -> int:
        """Set if not exists. Returns 1 if set, 0 otherwise."""
        result = self.set(key, value, nx=True)
        return 1 if result else 0
    
    def setex(self, key: str, seconds: int, value: str) -> str:
        """Set with expiration in seconds."""
        return self.set(key, value, ex=seconds)
    
    def psetex(self, key: str, milliseconds: int, value: str) -> str:
        """Set with expiration in milliseconds."""
        return self.set(key, value, px=milliseconds)
    
    def getset(self, key: str, value: str) -> Optional[str]:
        """Set new value and return old value."""
        old = self.get(key)
        self.set(key, value)
        return old
    
    def getdel(self, key: str) -> Optional[str]:
        """Get value and delete key."""
        value = self.get(key)
        if value is not None:
            self.store.delete(key)
        return value
    
    def append(self, key: str, value: str) -> int:
        """Append value to key. Returns new length."""
        if not self.store.check_type(key, DataType.STRING):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        current = self.store.get(key)
        if current is None:
            new_value = value
        else:
            new_value = str(current) + value
        
        self.store.set(key, new_value, DataType.STRING)
        return len(new_value)
    
    def strlen(self, key: str) -> int:
        """Get string length."""
        value = self.get(key)
        return len(value) if value else 0
    
    def getrange(self, key: str, start: int, end: int) -> str:
        """Get substring. Negative indices count from end."""
        value = self.get(key)
        if not value:
            return ""
        
        length = len(value)
        
        # Handle negative indices
        if start < 0:
            start = max(0, length + start)
        if end < 0:
            end = length + end
        
        # Redis includes end index
        end = min(end + 1, length)
        
        if start >= length or start > end:
            return ""
        
        return value[start:end]
    
    def setrange(self, key: str, offset: int, value: str) -> int:
        """Set substring at offset. Returns new length."""
        if not self.store.check_type(key, DataType.STRING):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        current = self.store.get(key) or ""
        current = str(current)
        
        # Pad with null bytes if needed
        if offset > len(current):
            current = current + '\x00' * (offset - len(current))
        
        new_value = current[:offset] + value + current[offset + len(value):]
        self.store.set(key, new_value, DataType.STRING)
        return len(new_value)
    
    def incr(self, key: str) -> int:
        """Increment by 1."""
        return self.incrby(key, 1)
    
    def incrby(self, key: str, increment: int) -> int:
        """Increment by amount."""
        if not self.store.check_type(key, DataType.STRING):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        value = self.store.get(key)
        if value is None:
            value = 0
        else:
            try:
                value = int(value)
            except ValueError:
                raise ValueError("ERR value is not an integer or out of range")
        
        new_value = value + increment
        self.store.set(key, str(new_value), DataType.STRING)
        return new_value
    
    def incrbyfloat(self, key: str, increment: float) -> str:
        """Increment by float amount."""
        if not self.store.check_type(key, DataType.STRING):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        value = self.store.get(key)
        if value is None:
            value = 0.0
        else:
            try:
                value = float(value)
            except ValueError:
                raise ValueError("ERR value is not a valid float")
        
        new_value = value + increment
        # Redis uses specific float formatting
        result = f"{new_value:.17g}"
        self.store.set(key, result, DataType.STRING)
        return result
    
    def decr(self, key: str) -> int:
        """Decrement by 1."""
        return self.incrby(key, -1)
    
    def decrby(self, key: str, decrement: int) -> int:
        """Decrement by amount."""
        return self.incrby(key, -decrement)
    
    def mget(self, *keys: str) -> List[Optional[str]]:
        """Get multiple keys."""
        return [self.get(key) for key in keys]
    
    def mset(self, *args: str) -> str:
        """Set multiple keys. Args: key1, val1, key2, val2, ..."""
        if len(args) % 2 != 0:
            raise ValueError("ERR wrong number of arguments for MSET")
        
        for i in range(0, len(args), 2):
            self.set(args[i], args[i + 1])
        return "OK"
    
    def msetnx(self, *args: str) -> int:
        """Set multiple keys if none exist. Returns 1 if all set, 0 otherwise."""
        if len(args) % 2 != 0:
            raise ValueError("ERR wrong number of arguments for MSETNX")
        
        # Check if any key exists
        for i in range(0, len(args), 2):
            if self.store.exists(args[i]) > 0:
                return 0
        
        # Set all keys
        for i in range(0, len(args), 2):
            self.set(args[i], args[i + 1])
        return 1
