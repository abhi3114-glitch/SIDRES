"""
Sorted Set data type handler for Sedris.

Implements Redis sorted set commands: ZADD, ZREM, ZRANGE, ZRANK, etc.
Uses a combination of dict and sorted list for O(log n) operations.
"""

import bisect
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ..store import DataStore

from ..store import DataType


class SortedSet:
    """
    Sorted set implementation using dict + sorted list.
    
    - member_scores: Dict[member, score] for O(1) score lookup
    - score_members: List of (score, member) tuples, sorted by score
    """
    
    def __init__(self):
        self.member_scores: Dict[str, float] = {}
        self.score_members: List[Tuple[float, str]] = []
    
    def add(self, score: float, member: str, nx: bool = False, xx: bool = False,
            gt: bool = False, lt: bool = False, ch: bool = False) -> int:
        """
        Add member with score.
        
        Returns:
            - If ch=False: 1 if new member added, 0 otherwise
            - If ch=True: 1 if member added or score changed, 0 otherwise
        """
        exists = member in self.member_scores
        
        # Check NX/XX conditions
        if nx and exists:
            return 0
        if xx and not exists:
            return 0
        
        # Check GT/LT conditions (only for updates)
        if exists:
            old_score = self.member_scores[member]
            if gt and score <= old_score:
                return 0
            if lt and score >= old_score:
                return 0
            
            # Remove old entry
            self._remove_from_sorted(old_score, member)
            changed = (old_score != score)
        else:
            changed = True
        
        # Add new entry
        self.member_scores[member] = score
        self._insert_sorted(score, member)
        
        if ch:
            return 1 if changed else 0
        return 0 if exists else 1
    
    def remove(self, member: str) -> bool:
        """Remove member. Returns True if removed."""
        if member not in self.member_scores:
            return False
        
        score = self.member_scores.pop(member)
        self._remove_from_sorted(score, member)
        return True
    
    def score(self, member: str) -> Optional[float]:
        """Get score of member."""
        return self.member_scores.get(member)
    
    def rank(self, member: str, reverse: bool = False) -> Optional[int]:
        """Get rank of member (0-indexed)."""
        if member not in self.member_scores:
            return None
        
        score = self.member_scores[member]
        idx = self._find_index(score, member)
        
        if reverse:
            return len(self.score_members) - 1 - idx
        return idx
    
    def range(self, start: int, stop: int, reverse: bool = False,
              withscores: bool = False) -> List:
        """Get range by rank."""
        length = len(self.score_members)
        
        # Handle negative indices
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        
        # Redis includes stop
        stop = min(stop + 1, length)
        
        if start >= length or start > stop:
            return []
        
        items = self.score_members[start:stop]
        if reverse:
            items = list(reversed(items))
        
        if withscores:
            result = []
            for score, member in items:
                result.extend([member, str(score)])
            return result
        
        return [member for score, member in items]
    
    def range_by_score(self, min_score: float, max_score: float,
                       withscores: bool = False, offset: int = 0,
                       count: int = -1, reverse: bool = False) -> List:
        """Get range by score."""
        if reverse:
            min_score, max_score = max_score, min_score
        
        # Find start and end positions
        start_idx = bisect.bisect_left(self.score_members, (min_score, ""))
        end_idx = bisect.bisect_right(self.score_members, (max_score, "\xff" * 100))
        
        items = self.score_members[start_idx:end_idx]
        
        if reverse:
            items = list(reversed(items))
        
        # Apply offset and count
        if offset > 0:
            items = items[offset:]
        if count >= 0:
            items = items[:count]
        
        if withscores:
            result = []
            for score, member in items:
                result.extend([member, str(score)])
            return result
        
        return [member for score, member in items]
    
    def count(self, min_score: float, max_score: float) -> int:
        """Count members with scores in range."""
        start_idx = bisect.bisect_left(self.score_members, (min_score, ""))
        end_idx = bisect.bisect_right(self.score_members, (max_score, "\xff" * 100))
        return end_idx - start_idx
    
    def card(self) -> int:
        """Get cardinality."""
        return len(self.member_scores)
    
    def incrby(self, member: str, increment: float) -> float:
        """Increment score by amount."""
        current = self.member_scores.get(member, 0)
        new_score = current + increment
        
        if member in self.member_scores:
            self._remove_from_sorted(current, member)
        
        self.member_scores[member] = new_score
        self._insert_sorted(new_score, member)
        return new_score
    
    def _insert_sorted(self, score: float, member: str) -> None:
        """Insert (score, member) maintaining sort order."""
        bisect.insort(self.score_members, (score, member))
    
    def _remove_from_sorted(self, score: float, member: str) -> None:
        """Remove (score, member) from sorted list."""
        idx = self._find_index(score, member)
        if idx is not None:
            del self.score_members[idx]
    
    def _find_index(self, score: float, member: str) -> Optional[int]:
        """Find index of (score, member) tuple."""
        left = bisect.bisect_left(self.score_members, (score, member))
        if left < len(self.score_members) and self.score_members[left] == (score, member):
            return left
        return None


class SortedSetHandler:
    """Handler for sorted set operations."""
    
    def __init__(self, store: "DataStore"):
        self.store = store
    
    def _get_zset(self, key: str, create: bool = False) -> Optional[SortedSet]:
        """Get sorted set value, optionally creating if not exists."""
        if not self.store.check_type(key, DataType.ZSET):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        
        if create:
            return self.store.get_or_create(key, DataType.ZSET, SortedSet)
        return self.store.get(key)
    
    def zadd(self, key: str, *args, nx: bool = False, xx: bool = False,
             gt: bool = False, lt: bool = False, ch: bool = False) -> int:
        """
        Add members with scores.
        Args: score1, member1, score2, member2, ...
        """
        if len(args) % 2 != 0:
            raise ValueError("ERR wrong number of arguments for ZADD")
        
        zset = self._get_zset(key, create=True)
        added = 0
        
        for i in range(0, len(args), 2):
            score = float(args[i])
            member = str(args[i + 1])
            added += zset.add(score, member, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch)
        
        return added
    
    def zrem(self, key: str, *members: str) -> int:
        """Remove members."""
        zset = self._get_zset(key)
        if not zset:
            return 0
        
        count = sum(1 for m in members if zset.remove(m))
        
        if zset.card() == 0:
            self.store.delete(key)
        
        return count
    
    def zscore(self, key: str, member: str) -> Optional[str]:
        """Get score of member."""
        zset = self._get_zset(key)
        if not zset:
            return None
        score = zset.score(member)
        return str(score) if score is not None else None
    
    def zrank(self, key: str, member: str) -> Optional[int]:
        """Get rank of member (low to high)."""
        zset = self._get_zset(key)
        if not zset:
            return None
        return zset.rank(member)
    
    def zrevrank(self, key: str, member: str) -> Optional[int]:
        """Get rank of member (high to low)."""
        zset = self._get_zset(key)
        if not zset:
            return None
        return zset.rank(member, reverse=True)
    
    def zrange(self, key: str, start: int, stop: int,
               withscores: bool = False, rev: bool = False) -> List:
        """Get range by rank."""
        zset = self._get_zset(key)
        if not zset:
            return []
        return zset.range(start, stop, reverse=rev, withscores=withscores)
    
    def zrevrange(self, key: str, start: int, stop: int,
                  withscores: bool = False) -> List:
        """Get range by rank, high to low."""
        result = self.zrange(key, start, stop, withscores=False)
        result = list(reversed(result))
        
        if withscores:
            zset = self._get_zset(key)
            scored = []
            for member in result:
                scored.extend([member, str(zset.score(member))])
            return scored
        
        return result
    
    def zrangebyscore(self, key: str, min_score: str, max_score: str,
                      withscores: bool = False, offset: int = 0,
                      count: int = -1) -> List:
        """Get range by score."""
        zset = self._get_zset(key)
        if not zset:
            return []
        
        min_val = self._parse_score(min_score)
        max_val = self._parse_score(max_score)
        
        return zset.range_by_score(min_val, max_val, withscores=withscores,
                                   offset=offset, count=count)
    
    def zrevrangebyscore(self, key: str, max_score: str, min_score: str,
                         withscores: bool = False, offset: int = 0,
                         count: int = -1) -> List:
        """Get range by score, high to low."""
        zset = self._get_zset(key)
        if not zset:
            return []
        
        min_val = self._parse_score(min_score)
        max_val = self._parse_score(max_score)
        
        return zset.range_by_score(min_val, max_val, withscores=withscores,
                                   offset=offset, count=count, reverse=True)
    
    def zcard(self, key: str) -> int:
        """Get cardinality."""
        zset = self._get_zset(key)
        return zset.card() if zset else 0
    
    def zcount(self, key: str, min_score: str, max_score: str) -> int:
        """Count members with scores in range."""
        zset = self._get_zset(key)
        if not zset:
            return 0
        
        min_val = self._parse_score(min_score)
        max_val = self._parse_score(max_score)
        
        return zset.count(min_val, max_val)
    
    def zincrby(self, key: str, increment: float, member: str) -> str:
        """Increment member's score."""
        zset = self._get_zset(key, create=True)
        new_score = zset.incrby(member, increment)
        return str(new_score)
    
    def zpopmin(self, key: str, count: int = 1) -> List:
        """Remove and return members with lowest scores."""
        zset = self._get_zset(key)
        if not zset or zset.card() == 0:
            return []
        
        result = []
        for _ in range(min(count, zset.card())):
            if zset.score_members:
                score, member = zset.score_members[0]
                zset.remove(member)
                result.extend([member, str(score)])
        
        if zset.card() == 0:
            self.store.delete(key)
        
        return result
    
    def zpopmax(self, key: str, count: int = 1) -> List:
        """Remove and return members with highest scores."""
        zset = self._get_zset(key)
        if not zset or zset.card() == 0:
            return []
        
        result = []
        for _ in range(min(count, zset.card())):
            if zset.score_members:
                score, member = zset.score_members[-1]
                zset.remove(member)
                result.extend([member, str(score)])
        
        if zset.card() == 0:
            self.store.delete(key)
        
        return result
    
    def zmscore(self, key: str, *members: str) -> List[Optional[str]]:
        """Get scores of multiple members."""
        zset = self._get_zset(key)
        if not zset:
            return [None] * len(members)
        
        return [str(zset.score(m)) if zset.score(m) is not None else None
                for m in members]
    
    def _parse_score(self, score_str: str) -> float:
        """Parse score string (handles -inf, +inf, exclusive ranges)."""
        score_str = score_str.strip()
        
        if score_str == "-inf":
            return float("-inf")
        if score_str in ("+inf", "inf"):
            return float("inf")
        
        # Handle exclusive range (starts with '(')
        if score_str.startswith("("):
            # For exclusive, we add/subtract a tiny amount
            val = float(score_str[1:])
            return val + 1e-10  # Approximate exclusive
        
        return float(score_str)
