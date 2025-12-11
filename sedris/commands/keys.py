"""
Key management commands for Sedris.
"""

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from . import CommandRegistry
    from ..store import DataStore


def register_key_commands(registry: "CommandRegistry", store: "DataStore"):
    """Register key management commands."""
    
    def cmd_del(*keys):
        """DEL key [key ...] - Delete keys."""
        return store.delete(*keys)
    
    def cmd_exists(*keys):
        """EXISTS key [key ...] - Check if keys exist."""
        return store.exists(*keys)
    
    def cmd_keys(pattern="*"):
        """KEYS pattern - Find keys matching pattern."""
        return store.keys(pattern)
    
    def cmd_type(key):
        """TYPE key - Get type of key."""
        return store.type(key)
    
    def cmd_rename(key, newkey):
        """RENAME key newkey - Rename a key."""
        if not store.rename(key, newkey):
            raise ValueError("ERR no such key")
        return "OK"
    
    def cmd_renamenx(key, newkey):
        """RENAMENX key newkey - Rename if newkey doesn't exist."""
        if store.exists(newkey) > 0:
            return 0
        if not store.rename(key, newkey):
            raise ValueError("ERR no such key")
        return 1
    
    def cmd_expire(key, seconds):
        """EXPIRE key seconds - Set TTL in seconds."""
        return 1 if store.expire(key, int(seconds)) else 0
    
    def cmd_expireat(key, timestamp):
        """EXPIREAT key timestamp - Set expiry as Unix timestamp."""
        return 1 if store.expireat(key, float(timestamp)) else 0
    
    def cmd_pexpire(key, milliseconds):
        """PEXPIRE key milliseconds - Set TTL in milliseconds."""
        return 1 if store.expire(key, int(milliseconds) // 1000) else 0
    
    def cmd_ttl(key):
        """TTL key - Get TTL in seconds."""
        return store.ttl(key)
    
    def cmd_pttl(key):
        """PTTL key - Get TTL in milliseconds."""
        return store.pttl(key)
    
    def cmd_persist(key):
        """PERSIST key - Remove expiration."""
        return 1 if store.persist(key) else 0
    
    def cmd_randomkey():
        """RANDOMKEY - Return random key."""
        return store.randomkey()
    
    def cmd_scan(cursor, *args):
        """SCAN cursor [MATCH pattern] [COUNT count] - Incrementally iterate keys."""
        import fnmatch
        
        cursor = int(cursor)
        pattern = "*"
        count = 10
        
        # Parse options
        i = 0
        while i < len(args):
            if args[i].upper() == "MATCH" and i + 1 < len(args):
                pattern = args[i + 1]
                i += 2
            elif args[i].upper() == "COUNT" and i + 1 < len(args):
                count = int(args[i + 1])
                i += 2
            else:
                i += 1
        
        all_keys = store.keys("*")
        
        # Filter by pattern
        if pattern != "*":
            all_keys = [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
        
        # Paginate
        start = cursor
        end = min(start + count, len(all_keys))
        
        if end >= len(all_keys):
            next_cursor = 0
        else:
            next_cursor = end
        
        return [str(next_cursor), all_keys[start:end]]
    
    def cmd_touch(*keys):
        """TOUCH key [key ...] - Update access time."""
        return store.exists(*keys)
    
    def cmd_unlink(*keys):
        """UNLINK key [key ...] - Delete keys (async-like)."""
        return store.delete(*keys)
    
    def cmd_dump(key):
        """DUMP key - Serialize key value."""
        import pickle
        import base64
        
        value = store.get(key)
        if value is None:
            return None
        
        data = pickle.dumps((store.type(key), value))
        return base64.b64encode(data).decode('ascii')
    
    def cmd_object(subcommand, *args):
        """OBJECT subcommand - Inspect object internals."""
        subcommand = subcommand.upper()
        
        if subcommand == "ENCODING":
            if not args:
                raise ValueError("ERR wrong number of arguments")
            key = args[0]
            dtype = store.type(key)
            # Return encoding based on type
            encodings = {
                "string": "embstr",
                "list": "listpack",
                "set": "listpack",
                "hash": "listpack",
                "zset": "listpack",
                "none": None
            }
            return encodings.get(dtype)
        elif subcommand == "FREQ":
            return 0  # Placeholder
        elif subcommand == "IDLETIME":
            return 0  # Placeholder
        elif subcommand == "REFCOUNT":
            return 1
        else:
            raise ValueError(f"ERR Unknown OBJECT subcommand '{subcommand}'")
    
    # Register commands
    registry.register("DEL", cmd_del, min_args=1)
    registry.register("EXISTS", cmd_exists, min_args=1)
    registry.register("KEYS", cmd_keys, min_args=0, max_args=1)
    registry.register("TYPE", cmd_type, min_args=1, max_args=1)
    registry.register("RENAME", cmd_rename, min_args=2, max_args=2)
    registry.register("RENAMENX", cmd_renamenx, min_args=2, max_args=2)
    registry.register("EXPIRE", cmd_expire, min_args=2, max_args=2)
    registry.register("EXPIREAT", cmd_expireat, min_args=2, max_args=2)
    registry.register("PEXPIRE", cmd_pexpire, min_args=2, max_args=2)
    registry.register("TTL", cmd_ttl, min_args=1, max_args=1)
    registry.register("PTTL", cmd_pttl, min_args=1, max_args=1)
    registry.register("PERSIST", cmd_persist, min_args=1, max_args=1)
    registry.register("RANDOMKEY", cmd_randomkey, min_args=0, max_args=0)
    registry.register("SCAN", cmd_scan, min_args=1)
    registry.register("TOUCH", cmd_touch, min_args=1)
    registry.register("UNLINK", cmd_unlink, min_args=1)
    registry.register("DUMP", cmd_dump, min_args=1, max_args=1)
    registry.register("OBJECT", cmd_object, min_args=1)
