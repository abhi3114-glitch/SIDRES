"""
String commands for Sedris.
"""

from typing import TYPE_CHECKING
from ..datatypes.strings import StringHandler

if TYPE_CHECKING:
    from . import CommandRegistry
    from ..store import DataStore


def register_string_commands(registry: "CommandRegistry", store: "DataStore"):
    """Register string commands."""
    
    handler = StringHandler(store)
    
    def cmd_get(key):
        """GET key - Get value."""
        return handler.get(key)
    
    def cmd_set(key, value, *args):
        """SET key value [EX seconds] [PX ms] [NX|XX] [KEEPTTL]"""
        ex = px = None
        nx = xx = keepttl = False
        get_old = False
        
        i = 0
        while i < len(args):
            arg = args[i].upper()
            if arg == "EX" and i + 1 < len(args):
                ex = int(args[i + 1])
                i += 2
            elif arg == "PX" and i + 1 < len(args):
                px = int(args[i + 1])
                i += 2
            elif arg == "EXAT" and i + 1 < len(args):
                import time
                ex = int(args[i + 1]) - int(time.time())
                i += 2
            elif arg == "PXAT" and i + 1 < len(args):
                import time
                px = int(args[i + 1]) - int(time.time() * 1000)
                i += 2
            elif arg == "NX":
                nx = True
                i += 1
            elif arg == "XX":
                xx = True
                i += 1
            elif arg == "KEEPTTL":
                keepttl = True
                i += 1
            elif arg == "GET":
                get_old = True
                i += 1
            else:
                i += 1
        
        if get_old:
            old_value = handler.get(key)
            result = handler.set(key, value, ex=ex, px=px, nx=nx, xx=xx, keepttl=keepttl)
            return old_value if result else None
        
        return handler.set(key, value, ex=ex, px=px, nx=nx, xx=xx, keepttl=keepttl)
    
    def cmd_setnx(key, value):
        """SETNX key value - Set if not exists."""
        return handler.setnx(key, value)
    
    def cmd_setex(key, seconds, value):
        """SETEX key seconds value - Set with TTL."""
        return handler.setex(key, int(seconds), value)
    
    def cmd_psetex(key, milliseconds, value):
        """PSETEX key ms value - Set with TTL in ms."""
        return handler.psetex(key, int(milliseconds), value)
    
    def cmd_getset(key, value):
        """GETSET key value - Set and return old value."""
        return handler.getset(key, value)
    
    def cmd_getdel(key):
        """GETDEL key - Get and delete."""
        return handler.getdel(key)
    
    def cmd_getex(key, *args):
        """GETEX key [EX seconds] [PX ms] [PERSIST]"""
        value = handler.get(key)
        if value is None:
            return None
        
        i = 0
        while i < len(args):
            arg = args[i].upper()
            if arg == "EX" and i + 1 < len(args):
                store.expire(key, int(args[i + 1]))
                i += 2
            elif arg == "PX" and i + 1 < len(args):
                store.expire(key, int(args[i + 1]) // 1000)
                i += 2
            elif arg == "EXAT" and i + 1 < len(args):
                store.expireat(key, float(args[i + 1]))
                i += 2
            elif arg == "PXAT" and i + 1 < len(args):
                store.expireat(key, float(args[i + 1]) / 1000)
                i += 2
            elif arg == "PERSIST":
                store.persist(key)
                i += 1
            else:
                i += 1
        
        return value
    
    def cmd_append(key, value):
        """APPEND key value - Append to string."""
        return handler.append(key, value)
    
    def cmd_strlen(key):
        """STRLEN key - Get string length."""
        return handler.strlen(key)
    
    def cmd_getrange(key, start, end):
        """GETRANGE key start end - Get substring."""
        return handler.getrange(key, int(start), int(end))
    
    def cmd_setrange(key, offset, value):
        """SETRANGE key offset value - Set substring."""
        return handler.setrange(key, int(offset), value)
    
    def cmd_incr(key):
        """INCR key - Increment by 1."""
        return handler.incr(key)
    
    def cmd_incrby(key, increment):
        """INCRBY key increment - Increment by amount."""
        return handler.incrby(key, int(increment))
    
    def cmd_incrbyfloat(key, increment):
        """INCRBYFLOAT key increment - Increment by float."""
        return handler.incrbyfloat(key, float(increment))
    
    def cmd_decr(key):
        """DECR key - Decrement by 1."""
        return handler.decr(key)
    
    def cmd_decrby(key, decrement):
        """DECRBY key decrement - Decrement by amount."""
        return handler.decrby(key, int(decrement))
    
    def cmd_mget(*keys):
        """MGET key [key ...] - Get multiple keys."""
        return handler.mget(*keys)
    
    def cmd_mset(*args):
        """MSET key value [key value ...] - Set multiple keys."""
        return handler.mset(*args)
    
    def cmd_msetnx(*args):
        """MSETNX key value [...] - Set multiple if none exist."""
        return handler.msetnx(*args)
    
    def cmd_getdel(key):
        """GETDEL key - Get value and delete key."""
        return handler.getdel(key)
    
    # Register commands
    registry.register("GET", cmd_get, min_args=1, max_args=1)
    registry.register("SET", cmd_set, min_args=2)
    registry.register("SETNX", cmd_setnx, min_args=2, max_args=2)
    registry.register("SETEX", cmd_setex, min_args=3, max_args=3)
    registry.register("PSETEX", cmd_psetex, min_args=3, max_args=3)
    registry.register("GETSET", cmd_getset, min_args=2, max_args=2)
    registry.register("GETDEL", cmd_getdel, min_args=1, max_args=1)
    registry.register("GETEX", cmd_getex, min_args=1)
    registry.register("APPEND", cmd_append, min_args=2, max_args=2)
    registry.register("STRLEN", cmd_strlen, min_args=1, max_args=1)
    registry.register("GETRANGE", cmd_getrange, min_args=3, max_args=3)
    registry.register("SETRANGE", cmd_setrange, min_args=3, max_args=3)
    registry.register("INCR", cmd_incr, min_args=1, max_args=1)
    registry.register("INCRBY", cmd_incrby, min_args=2, max_args=2)
    registry.register("INCRBYFLOAT", cmd_incrbyfloat, min_args=2, max_args=2)
    registry.register("DECR", cmd_decr, min_args=1, max_args=1)
    registry.register("DECRBY", cmd_decrby, min_args=2, max_args=2)
    registry.register("MGET", cmd_mget, min_args=1)
    registry.register("MSET", cmd_mset, min_args=2)
    registry.register("MSETNX", cmd_msetnx, min_args=2)
    
    # Alias
    registry.register("SUBSTR", cmd_getrange, min_args=3, max_args=3)
