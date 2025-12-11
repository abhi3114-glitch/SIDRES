"""
Sorted Set commands for Sedris.
"""

from typing import TYPE_CHECKING
from ..datatypes.sorted_sets import SortedSetHandler

if TYPE_CHECKING:
    from . import CommandRegistry
    from ..store import DataStore


def register_zset_commands(registry: "CommandRegistry", store: "DataStore"):
    """Register sorted set commands."""
    
    handler = SortedSetHandler(store)
    
    def cmd_zadd(key, *args):
        """ZADD key [NX|XX] [GT|LT] [CH] score member [...]."""
        nx = xx = gt = lt = ch = False
        score_members = []
        
        i = 0
        while i < len(args):
            arg = args[i].upper()
            if arg == "NX":
                nx = True
                i += 1
            elif arg == "XX":
                xx = True
                i += 1
            elif arg == "GT":
                gt = True
                i += 1
            elif arg == "LT":
                lt = True
                i += 1
            elif arg == "CH":
                ch = True
                i += 1
            else:
                # Rest are score-member pairs
                score_members = list(args[i:])
                break
        
        return handler.zadd(key, *score_members, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch)
    
    def cmd_zrem(key, *members):
        """ZREM key member [member ...] - Remove members."""
        return handler.zrem(key, *members)
    
    def cmd_zscore(key, member):
        """ZSCORE key member - Get score."""
        return handler.zscore(key, member)
    
    def cmd_zrank(key, member, *args):
        """ZRANK key member [WITHSCORE] - Get rank (low to high)."""
        rank = handler.zrank(key, member)
        if rank is None:
            return None
        
        if args and args[0].upper() == "WITHSCORE":
            score = handler.zscore(key, member)
            return [rank, score]
        return rank
    
    def cmd_zrevrank(key, member, *args):
        """ZREVRANK key member [WITHSCORE] - Get rank (high to low)."""
        rank = handler.zrevrank(key, member)
        if rank is None:
            return None
        
        if args and args[0].upper() == "WITHSCORE":
            score = handler.zscore(key, member)
            return [rank, score]
        return rank
    
    def cmd_zrange(key, start, stop, *args):
        """ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]."""
        withscores = False
        rev = False
        byscore = False
        offset = 0
        count = -1
        
        i = 0
        while i < len(args):
            arg = args[i].upper()
            if arg == "WITHSCORES":
                withscores = True
                i += 1
            elif arg == "REV":
                rev = True
                i += 1
            elif arg == "BYSCORE":
                byscore = True
                i += 1
            elif arg == "LIMIT" and i + 2 < len(args):
                offset = int(args[i + 1])
                count = int(args[i + 2])
                i += 3
            else:
                i += 1
        
        if byscore:
            if rev:
                return handler.zrevrangebyscore(key, start, stop, withscores=withscores,
                                                offset=offset, count=count)
            return handler.zrangebyscore(key, start, stop, withscores=withscores,
                                         offset=offset, count=count)
        
        return handler.zrange(key, int(start), int(stop), withscores=withscores, rev=rev)
    
    def cmd_zrevrange(key, start, stop, *args):
        """ZREVRANGE key start stop [WITHSCORES]."""
        withscores = "WITHSCORES" in [a.upper() for a in args]
        return handler.zrevrange(key, int(start), int(stop), withscores=withscores)
    
    def cmd_zrangebyscore(key, min_score, max_score, *args):
        """ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]."""
        withscores = False
        offset = 0
        count = -1
        
        i = 0
        while i < len(args):
            arg = args[i].upper()
            if arg == "WITHSCORES":
                withscores = True
                i += 1
            elif arg == "LIMIT" and i + 2 < len(args):
                offset = int(args[i + 1])
                count = int(args[i + 2])
                i += 3
            else:
                i += 1
        
        return handler.zrangebyscore(key, min_score, max_score, withscores=withscores,
                                     offset=offset, count=count)
    
    def cmd_zrevrangebyscore(key, max_score, min_score, *args):
        """ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]."""
        withscores = False
        offset = 0
        count = -1
        
        i = 0
        while i < len(args):
            arg = args[i].upper()
            if arg == "WITHSCORES":
                withscores = True
                i += 1
            elif arg == "LIMIT" and i + 2 < len(args):
                offset = int(args[i + 1])
                count = int(args[i + 2])
                i += 3
            else:
                i += 1
        
        return handler.zrevrangebyscore(key, max_score, min_score, withscores=withscores,
                                        offset=offset, count=count)
    
    def cmd_zcard(key):
        """ZCARD key - Get cardinality."""
        return handler.zcard(key)
    
    def cmd_zcount(key, min_score, max_score):
        """ZCOUNT key min max - Count in score range."""
        return handler.zcount(key, min_score, max_score)
    
    def cmd_zincrby(key, increment, member):
        """ZINCRBY key increment member - Increment score."""
        return handler.zincrby(key, float(increment), member)
    
    def cmd_zpopmin(key, *args):
        """ZPOPMIN key [count] - Pop members with min scores."""
        count = int(args[0]) if args else 1
        return handler.zpopmin(key, count)
    
    def cmd_zpopmax(key, *args):
        """ZPOPMAX key [count] - Pop members with max scores."""
        count = int(args[0]) if args else 1
        return handler.zpopmax(key, count)
    
    def cmd_zmscore(key, *members):
        """ZMSCORE key member [member ...] - Get multiple scores."""
        return handler.zmscore(key, *members)
    
    def cmd_zrangestore(dest, src, start, stop, *args):
        """ZRANGESTORE dest src start stop [...] - Store range result."""
        # Get range then store
        byscore = "BYSCORE" in [a.upper() for a in args]
        rev = "REV" in [a.upper() for a in args]
        
        if byscore:
            if rev:
                members = handler.zrevrangebyscore(src, start, stop, withscores=True)
            else:
                members = handler.zrangebyscore(src, start, stop, withscores=True)
        else:
            members = handler.zrange(src, int(start), int(stop), withscores=True, rev=rev)
        
        if not members:
            store.delete(dest)
            return 0
        
        # members is [member, score, member, score, ...]
        add_args = []
        for i in range(0, len(members), 2):
            add_args.extend([members[i + 1], members[i]])  # score, member
        
        handler.zadd(dest, *add_args)
        return len(members) // 2
    
    def cmd_zscan(key, cursor, *args):
        """ZSCAN key cursor [MATCH pattern] [COUNT count]."""
        import fnmatch
        
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
        
        # Get all members with scores
        all_members = handler.zrange(key, 0, -1, withscores=True)
        
        # Filter by pattern (apply to members)
        filtered = []
        for i in range(0, len(all_members), 2):
            member = all_members[i]
            score = all_members[i + 1]
            if match == "*" or fnmatch.fnmatch(member, match):
                filtered.extend([member, score])
        
        # Paginate
        cursor = int(cursor)
        start = cursor * 2  # Each entry is 2 elements
        end = min(start + count * 2, len(filtered))
        
        if end >= len(filtered):
            next_cursor = 0
        else:
            next_cursor = end // 2
        
        return [str(next_cursor), filtered[start:end]]
    
    # Blocking commands (non-blocking implementation)
    def cmd_bzpopmin(*args):
        """BZPOPMIN key [key ...] timeout - Blocking ZPOPMIN."""
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        keys = args[:-1]
        
        for key in keys:
            result = handler.zpopmin(key, 1)
            if result:
                return [key] + result
        return None
    
    def cmd_bzpopmax(*args):
        """BZPOPMAX key [key ...] timeout - Blocking ZPOPMAX."""
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        keys = args[:-1]
        
        for key in keys:
            result = handler.zpopmax(key, 1)
            if result:
                return [key] + result
        return None
    
    # Register commands
    registry.register("ZADD", cmd_zadd, min_args=3)
    registry.register("ZREM", cmd_zrem, min_args=2)
    registry.register("ZSCORE", cmd_zscore, min_args=2, max_args=2)
    registry.register("ZRANK", cmd_zrank, min_args=2)
    registry.register("ZREVRANK", cmd_zrevrank, min_args=2)
    registry.register("ZRANGE", cmd_zrange, min_args=3)
    registry.register("ZREVRANGE", cmd_zrevrange, min_args=3)
    registry.register("ZRANGEBYSCORE", cmd_zrangebyscore, min_args=3)
    registry.register("ZREVRANGEBYSCORE", cmd_zrevrangebyscore, min_args=3)
    registry.register("ZCARD", cmd_zcard, min_args=1, max_args=1)
    registry.register("ZCOUNT", cmd_zcount, min_args=3, max_args=3)
    registry.register("ZINCRBY", cmd_zincrby, min_args=3, max_args=3)
    registry.register("ZPOPMIN", cmd_zpopmin, min_args=1)
    registry.register("ZPOPMAX", cmd_zpopmax, min_args=1)
    registry.register("ZMSCORE", cmd_zmscore, min_args=2)
    registry.register("ZRANGESTORE", cmd_zrangestore, min_args=4)
    registry.register("ZSCAN", cmd_zscan, min_args=2)
    registry.register("BZPOPMIN", cmd_bzpopmin, min_args=2)
    registry.register("BZPOPMAX", cmd_bzpopmax, min_args=2)
