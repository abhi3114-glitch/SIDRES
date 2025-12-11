"""
Hash commands for Sedris.
"""

from typing import TYPE_CHECKING
from ..datatypes.hashes import HashHandler

if TYPE_CHECKING:
    from . import CommandRegistry
    from ..store import DataStore


def register_hash_commands(registry: "CommandRegistry", store: "DataStore"):
    """Register hash commands."""
    
    handler = HashHandler(store)
    
    def cmd_hset(key, *args):
        """HSET key field value [field value ...] - Set fields."""
        return handler.hset(key, *args)
    
    def cmd_hsetnx(key, field, value):
        """HSETNX key field value - Set if not exists."""
        return handler.hsetnx(key, field, value)
    
    def cmd_hget(key, field):
        """HGET key field - Get field value."""
        return handler.hget(key, field)
    
    def cmd_hmset(key, *args):
        """HMSET key field value [...] - Set multiple (deprecated)."""
        return handler.hmset(key, *args)
    
    def cmd_hmget(key, *fields):
        """HMGET key field [field ...] - Get multiple fields."""
        return handler.hmget(key, *fields)
    
    def cmd_hdel(key, *fields):
        """HDEL key field [field ...] - Delete fields."""
        return handler.hdel(key, *fields)
    
    def cmd_hexists(key, field):
        """HEXISTS key field - Check if field exists."""
        return handler.hexists(key, field)
    
    def cmd_hlen(key):
        """HLEN key - Get number of fields."""
        return handler.hlen(key)
    
    def cmd_hkeys(key):
        """HKEYS key - Get all field names."""
        return handler.hkeys(key)
    
    def cmd_hvals(key):
        """HVALS key - Get all values."""
        return handler.hvals(key)
    
    def cmd_hgetall(key):
        """HGETALL key - Get all fields and values."""
        result = handler.hgetall(key)
        # Flatten dict to list
        flat = []
        for k, v in result.items():
            flat.extend([k, v])
        return flat
    
    def cmd_hincrby(key, field, increment):
        """HINCRBY key field increment - Increment by integer."""
        return handler.hincrby(key, field, int(increment))
    
    def cmd_hincrbyfloat(key, field, increment):
        """HINCRBYFLOAT key field increment - Increment by float."""
        return handler.hincrbyfloat(key, field, float(increment))
    
    def cmd_hstrlen(key, field):
        """HSTRLEN key field - Get field value length."""
        return handler.hstrlen(key, field)
    
    def cmd_hrandfield(key, *args):
        """HRANDFIELD key [count [WITHVALUES]]."""
        count = None
        withvalues = False
        
        if args:
            count = int(args[0])
            if len(args) > 1 and args[1].upper() == "WITHVALUES":
                withvalues = True
        
        return handler.hrandfield(key, count=count, withvalues=withvalues)
    
    def cmd_hscan(key, cursor, *args):
        """HSCAN key cursor [MATCH pattern] [COUNT count]."""
        match = "*"
        count = 10
        
        i = 0
        while i < len(args):
            opt = args[i].upper()
            if opt == "MATCH" and i + 1 < len(args):
                match = args[i + 1]
                i += 2
            elif opt == "COUNT" and i + 1 < len(args):
                count = int(args[i + 1])
                i += 2
            else:
                i += 1
        
        next_cursor, pairs = handler.hscan(key, int(cursor), match=match, count=count)
        return [str(next_cursor), pairs]
    
    # Register commands
    registry.register("HSET", cmd_hset, min_args=3)
    registry.register("HSETNX", cmd_hsetnx, min_args=3, max_args=3)
    registry.register("HGET", cmd_hget, min_args=2, max_args=2)
    registry.register("HMSET", cmd_hmset, min_args=3)
    registry.register("HMGET", cmd_hmget, min_args=2)
    registry.register("HDEL", cmd_hdel, min_args=2)
    registry.register("HEXISTS", cmd_hexists, min_args=2, max_args=2)
    registry.register("HLEN", cmd_hlen, min_args=1, max_args=1)
    registry.register("HKEYS", cmd_hkeys, min_args=1, max_args=1)
    registry.register("HVALS", cmd_hvals, min_args=1, max_args=1)
    registry.register("HGETALL", cmd_hgetall, min_args=1, max_args=1)
    registry.register("HINCRBY", cmd_hincrby, min_args=3, max_args=3)
    registry.register("HINCRBYFLOAT", cmd_hincrbyfloat, min_args=3, max_args=3)
    registry.register("HSTRLEN", cmd_hstrlen, min_args=2, max_args=2)
    registry.register("HRANDFIELD", cmd_hrandfield, min_args=1)
    registry.register("HSCAN", cmd_hscan, min_args=2)
