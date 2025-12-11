"""
Set commands for Sedris.
"""

from typing import TYPE_CHECKING
from ..datatypes.sets import SetHandler

if TYPE_CHECKING:
    from . import CommandRegistry
    from ..store import DataStore


def register_set_commands(registry: "CommandRegistry", store: "DataStore"):
    """Register set commands."""
    
    handler = SetHandler(store)
    
    def cmd_sadd(key, *members):
        """SADD key member [member ...] - Add members."""
        return handler.sadd(key, *members)
    
    def cmd_srem(key, *members):
        """SREM key member [member ...] - Remove members."""
        return handler.srem(key, *members)
    
    def cmd_sismember(key, member):
        """SISMEMBER key member - Check if member exists."""
        return handler.sismember(key, member)
    
    def cmd_smismember(key, *members):
        """SMISMEMBER key member [...] - Check multiple members."""
        return handler.smismember(key, *members)
    
    def cmd_smembers(key):
        """SMEMBERS key - Get all members."""
        return handler.smembers(key)
    
    def cmd_scard(key):
        """SCARD key - Get set size."""
        return handler.scard(key)
    
    def cmd_spop(key, *args):
        """SPOP key [count] - Remove and return random members."""
        count = int(args[0]) if args else 1
        return handler.spop(key, count)
    
    def cmd_srandmember(key, *args):
        """SRANDMEMBER key [count] - Get random members."""
        count = int(args[0]) if args else None
        return handler.srandmember(key, count)
    
    def cmd_smove(source, destination, member):
        """SMOVE source dest member - Move member between sets."""
        return handler.smove(source, destination, member)
    
    def cmd_sunion(*keys):
        """SUNION key [key ...] - Return union."""
        return handler.sunion(*keys)
    
    def cmd_sunionstore(destination, *keys):
        """SUNIONSTORE dest key [...] - Store union."""
        return handler.sunionstore(destination, *keys)
    
    def cmd_sinter(*keys):
        """SINTER key [key ...] - Return intersection."""
        return handler.sinter(*keys)
    
    def cmd_sinterstore(destination, *keys):
        """SINTERSTORE dest key [...] - Store intersection."""
        return handler.sinterstore(destination, *keys)
    
    def cmd_sintercard(numkeys, *args):
        """SINTERCARD numkeys key [...] [LIMIT limit]."""
        numkeys = int(numkeys)
        
        # Parse LIMIT option
        limit = 0
        keys = []
        i = 0
        while i < len(args):
            if args[i].upper() == "LIMIT" and i + 1 < len(args):
                limit = int(args[i + 1])
                break
            keys.append(args[i])
            i += 1
        
        return handler.sintercard(numkeys, *keys[:numkeys], limit=limit)
    
    def cmd_sdiff(*keys):
        """SDIFF key [key ...] - Return difference."""
        return handler.sdiff(*keys)
    
    def cmd_sdiffstore(destination, *keys):
        """SDIFFSTORE dest key [...] - Store difference."""
        return handler.sdiffstore(destination, *keys)
    
    def cmd_sscan(key, cursor, *args):
        """SSCAN key cursor [MATCH pattern] [COUNT count]."""
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
        
        next_cursor, members = handler.sscan(key, int(cursor), match=match, count=count)
        return [str(next_cursor), members]
    
    # Register commands
    registry.register("SADD", cmd_sadd, min_args=2)
    registry.register("SREM", cmd_srem, min_args=2)
    registry.register("SISMEMBER", cmd_sismember, min_args=2, max_args=2)
    registry.register("SMISMEMBER", cmd_smismember, min_args=2)
    registry.register("SMEMBERS", cmd_smembers, min_args=1, max_args=1)
    registry.register("SCARD", cmd_scard, min_args=1, max_args=1)
    registry.register("SPOP", cmd_spop, min_args=1)
    registry.register("SRANDMEMBER", cmd_srandmember, min_args=1)
    registry.register("SMOVE", cmd_smove, min_args=3, max_args=3)
    registry.register("SUNION", cmd_sunion, min_args=1)
    registry.register("SUNIONSTORE", cmd_sunionstore, min_args=2)
    registry.register("SINTER", cmd_sinter, min_args=1)
    registry.register("SINTERSTORE", cmd_sinterstore, min_args=2)
    registry.register("SINTERCARD", cmd_sintercard, min_args=2)
    registry.register("SDIFF", cmd_sdiff, min_args=1)
    registry.register("SDIFFSTORE", cmd_sdiffstore, min_args=2)
    registry.register("SSCAN", cmd_sscan, min_args=2)
