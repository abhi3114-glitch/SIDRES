"""
List commands for Sedris.
"""

from typing import TYPE_CHECKING
from ..datatypes.lists import ListHandler

if TYPE_CHECKING:
    from . import CommandRegistry
    from ..store import DataStore


def register_list_commands(registry: "CommandRegistry", store: "DataStore"):
    """Register list commands."""
    
    handler = ListHandler(store)
    
    def cmd_lpush(key, *values):
        """LPUSH key value [value ...] - Insert at head."""
        return handler.lpush(key, *values)
    
    def cmd_lpushx(key, *values):
        """LPUSHX key value [...] - Insert at head if exists."""
        return handler.lpushx(key, *values)
    
    def cmd_rpush(key, *values):
        """RPUSH key value [value ...] - Insert at tail."""
        return handler.rpush(key, *values)
    
    def cmd_rpushx(key, *values):
        """RPUSHX key value [...] - Insert at tail if exists."""
        return handler.rpushx(key, *values)
    
    def cmd_lpop(key, *args):
        """LPOP key [count] - Remove from head."""
        count = int(args[0]) if args else 1
        result = handler.lpop(key, count)
        if count > 1 and result:
            return result
        return result
    
    def cmd_rpop(key, *args):
        """RPOP key [count] - Remove from tail."""
        count = int(args[0]) if args else 1
        result = handler.rpop(key, count)
        if count > 1 and result:
            return result
        return result
    
    def cmd_llen(key):
        """LLEN key - Get list length."""
        return handler.llen(key)
    
    def cmd_lrange(key, start, stop):
        """LRANGE key start stop - Get range."""
        return handler.lrange(key, int(start), int(stop))
    
    def cmd_lindex(key, index):
        """LINDEX key index - Get element at index."""
        return handler.lindex(key, int(index))
    
    def cmd_lset(key, index, value):
        """LSET key index value - Set element at index."""
        return handler.lset(key, int(index), value)
    
    def cmd_linsert(key, position, pivot, value):
        """LINSERT key BEFORE|AFTER pivot value."""
        return handler.linsert(key, position, pivot, value)
    
    def cmd_lrem(key, count, value):
        """LREM key count value - Remove occurrences."""
        return handler.lrem(key, int(count), value)
    
    def cmd_ltrim(key, start, stop):
        """LTRIM key start stop - Trim list."""
        return handler.ltrim(key, int(start), int(stop))
    
    def cmd_rpoplpush(source, destination):
        """RPOPLPUSH source dest - Pop from tail, push to head."""
        return handler.rpoplpush(source, destination)
    
    def cmd_lmove(source, dest, wherefrom, whereto):
        """LMOVE source dest LEFT|RIGHT LEFT|RIGHT."""
        return handler.lmove(source, dest, wherefrom, whereto)
    
    def cmd_lpos(key, element, *args):
        """LPOS key element [RANK rank] [COUNT count] [MAXLEN len]."""
        rank = 1
        count_opt = 1
        maxlen = 0
        
        i = 0
        while i < len(args):
            opt = args[i].upper()
            if opt == "RANK" and i + 1 < len(args):
                rank = int(args[i + 1])
                i += 2
            elif opt == "COUNT" and i + 1 < len(args):
                count_opt = int(args[i + 1])
                i += 2
            elif opt == "MAXLEN" and i + 1 < len(args):
                maxlen = int(args[i + 1])
                i += 2
            else:
                i += 1
        
        return handler.lpos(key, element, rank=rank, count=count_opt, maxlen=maxlen)
    
    def cmd_blpop(*args):
        """BLPOP key [key ...] timeout - Blocking LPOP."""
        # Non-blocking implementation (returns None if empty)
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        keys = args[:-1]
        # timeout = float(args[-1])  # Ignored in non-blocking
        
        for key in keys:
            result = handler.lpop(key, 1)
            if result is not None:
                return [key, result]
        return None
    
    def cmd_brpop(*args):
        """BRPOP key [key ...] timeout - Blocking RPOP."""
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        keys = args[:-1]
        
        for key in keys:
            result = handler.rpop(key, 1)
            if result is not None:
                return [key, result]
        return None
    
    def cmd_brpoplpush(source, dest, timeout):
        """BRPOPLPUSH source dest timeout - Blocking RPOPLPUSH."""
        return handler.rpoplpush(source, dest)
    
    def cmd_blmove(source, dest, wherefrom, whereto, timeout):
        """BLMOVE source dest LEFT|RIGHT LEFT|RIGHT timeout."""
        return handler.lmove(source, dest, wherefrom, whereto)
    
    # Register commands
    registry.register("LPUSH", cmd_lpush, min_args=2)
    registry.register("LPUSHX", cmd_lpushx, min_args=2)
    registry.register("RPUSH", cmd_rpush, min_args=2)
    registry.register("RPUSHX", cmd_rpushx, min_args=2)
    registry.register("LPOP", cmd_lpop, min_args=1)
    registry.register("RPOP", cmd_rpop, min_args=1)
    registry.register("LLEN", cmd_llen, min_args=1, max_args=1)
    registry.register("LRANGE", cmd_lrange, min_args=3, max_args=3)
    registry.register("LINDEX", cmd_lindex, min_args=2, max_args=2)
    registry.register("LSET", cmd_lset, min_args=3, max_args=3)
    registry.register("LINSERT", cmd_linsert, min_args=4, max_args=4)
    registry.register("LREM", cmd_lrem, min_args=3, max_args=3)
    registry.register("LTRIM", cmd_ltrim, min_args=3, max_args=3)
    registry.register("RPOPLPUSH", cmd_rpoplpush, min_args=2, max_args=2)
    registry.register("LMOVE", cmd_lmove, min_args=4, max_args=4)
    registry.register("LPOS", cmd_lpos, min_args=2)
    registry.register("BLPOP", cmd_blpop, min_args=2)
    registry.register("BRPOP", cmd_brpop, min_args=2)
    registry.register("BRPOPLPUSH", cmd_brpoplpush, min_args=3, max_args=3)
    registry.register("BLMOVE", cmd_blmove, min_args=5, max_args=5)
