"""
Pub/Sub module for Sedris.

Implements publish/subscribe messaging with pattern matching.
"""

import asyncio
import fnmatch
import logging
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from .protocol import RESPEncoder

logger = logging.getLogger("sedris")


class PubSubManager:
    """Manages publish/subscribe channels and subscribers."""
    
    def __init__(self):
        # channel -> set of subscriber callbacks
        self._channels: Dict[str, Set[Callable]] = defaultdict(set)
        # pattern -> set of subscriber callbacks
        self._patterns: Dict[str, Set[Callable]] = defaultdict(set)
        # subscriber -> set of subscribed channels
        self._subscriber_channels: Dict[int, Set[str]] = defaultdict(set)
        # subscriber -> set of subscribed patterns
        self._subscriber_patterns: Dict[int, Set[str]] = defaultdict(set)
    
    def subscribe(self, subscriber_id: int, channel: str, callback: Callable) -> int:
        """
        Subscribe to a channel.
        Returns total number of subscriptions for this subscriber.
        """
        self._channels[channel].add(callback)
        self._subscriber_channels[subscriber_id].add(channel)
        
        return len(self._subscriber_channels[subscriber_id]) + \
               len(self._subscriber_patterns[subscriber_id])
    
    def unsubscribe(self, subscriber_id: int, channel: str = None) -> int:
        """
        Unsubscribe from a channel or all channels.
        Returns total remaining subscriptions.
        """
        if channel is None:
            # Unsubscribe from all channels
            channels = list(self._subscriber_channels.get(subscriber_id, set()))
            for ch in channels:
                self._unsubscribe_channel(subscriber_id, ch)
        else:
            self._unsubscribe_channel(subscriber_id, channel)
        
        return len(self._subscriber_channels[subscriber_id]) + \
               len(self._subscriber_patterns[subscriber_id])
    
    def _unsubscribe_channel(self, subscriber_id: int, channel: str):
        """Unsubscribe from a specific channel."""
        self._subscriber_channels[subscriber_id].discard(channel)
        
        # Find and remove the callback
        if channel in self._channels:
            # We need to track callbacks by subscriber_id
            # For simplicity, we'll clear if empty
            if not self._subscriber_channels[subscriber_id]:
                self._channels[channel] = {
                    cb for cb in self._channels[channel]
                    if getattr(cb, '_subscriber_id', None) != subscriber_id
                }
    
    def psubscribe(self, subscriber_id: int, pattern: str, callback: Callable) -> int:
        """
        Subscribe to a pattern.
        Returns total number of subscriptions.
        """
        self._patterns[pattern].add(callback)
        self._subscriber_patterns[subscriber_id].add(pattern)
        
        return len(self._subscriber_channels[subscriber_id]) + \
               len(self._subscriber_patterns[subscriber_id])
    
    def punsubscribe(self, subscriber_id: int, pattern: str = None) -> int:
        """
        Unsubscribe from a pattern or all patterns.
        Returns total remaining subscriptions.
        """
        if pattern is None:
            patterns = list(self._subscriber_patterns.get(subscriber_id, set()))
            for pat in patterns:
                self._unsubscribe_pattern(subscriber_id, pat)
        else:
            self._unsubscribe_pattern(subscriber_id, pattern)
        
        return len(self._subscriber_channels[subscriber_id]) + \
               len(self._subscriber_patterns[subscriber_id])
    
    def _unsubscribe_pattern(self, subscriber_id: int, pattern: str):
        """Unsubscribe from a specific pattern."""
        self._subscriber_patterns[subscriber_id].discard(pattern)
        if pattern in self._patterns:
            self._patterns[pattern] = {
                cb for cb in self._patterns[pattern]
                if getattr(cb, '_subscriber_id', None) != subscriber_id
            }
    
    def publish(self, channel: str, message: str) -> int:
        """
        Publish a message to a channel.
        Returns number of subscribers that received the message.
        """
        count = 0
        
        # Notify direct channel subscribers
        for callback in self._channels.get(channel, set()):
            try:
                callback("message", channel, message)
                count += 1
            except Exception as e:
                logger.error(f"Publish callback error: {e}")
        
        # Notify pattern subscribers
        for pattern, callbacks in self._patterns.items():
            if fnmatch.fnmatch(channel, pattern):
                for callback in callbacks:
                    try:
                        callback("pmessage", pattern, channel, message)
                        count += 1
                    except Exception as e:
                        logger.error(f"Publish callback error: {e}")
        
        return count
    
    def numsub(self, *channels: str) -> List:
        """Get number of subscribers for channels."""
        result = []
        for channel in channels:
            count = len(self._channels.get(channel, set()))
            result.extend([channel, count])
        return result
    
    def numpat(self) -> int:
        """Get number of pattern subscriptions."""
        return sum(len(callbacks) for callbacks in self._patterns.values())
    
    def channels(self, pattern: str = "*") -> List[str]:
        """List active channels matching pattern."""
        if pattern == "*":
            return list(self._channels.keys())
        return [ch for ch in self._channels.keys() if fnmatch.fnmatch(ch, pattern)]


class PubSubClient:
    """
    Client-side pub/sub handler.
    
    Tracks subscriptions and handles messages for a single client.
    """
    
    def __init__(self, client_id: int, pubsub_manager: PubSubManager,
                 message_callback: Callable):
        self.client_id = client_id
        self.pubsub = pubsub_manager
        self.message_callback = message_callback
        self.subscribed_channels: Set[str] = set()
        self.subscribed_patterns: Set[str] = set()
    
    def subscribe(self, *channels: str) -> List:
        """Subscribe to channels."""
        results = []
        for channel in channels:
            if channel not in self.subscribed_channels:
                self.subscribed_channels.add(channel)
                
                # Create callback
                def on_message(msg_type, *args, _channel=channel):
                    self.message_callback(msg_type, *args)
                
                on_message._subscriber_id = self.client_id
                count = self.pubsub.subscribe(self.client_id, channel, on_message)
            else:
                count = len(self.subscribed_channels) + len(self.subscribed_patterns)
            
            results.append(["subscribe", channel, count])
        
        return results
    
    def unsubscribe(self, *channels: str) -> List:
        """Unsubscribe from channels."""
        if not channels:
            channels = tuple(self.subscribed_channels)
        
        results = []
        for channel in channels:
            self.subscribed_channels.discard(channel)
            count = self.pubsub.unsubscribe(self.client_id, channel)
            results.append(["unsubscribe", channel, count])
        
        return results
    
    def psubscribe(self, *patterns: str) -> List:
        """Subscribe to patterns."""
        results = []
        for pattern in patterns:
            if pattern not in self.subscribed_patterns:
                self.subscribed_patterns.add(pattern)
                
                def on_message(msg_type, *args, _pattern=pattern):
                    self.message_callback(msg_type, *args)
                
                on_message._subscriber_id = self.client_id
                count = self.pubsub.psubscribe(self.client_id, pattern, on_message)
            else:
                count = len(self.subscribed_channels) + len(self.subscribed_patterns)
            
            results.append(["psubscribe", pattern, count])
        
        return results
    
    def punsubscribe(self, *patterns: str) -> List:
        """Unsubscribe from patterns."""
        if not patterns:
            patterns = tuple(self.subscribed_patterns)
        
        results = []
        for pattern in patterns:
            self.subscribed_patterns.discard(pattern)
            count = self.pubsub.punsubscribe(self.client_id, pattern)
            results.append(["punsubscribe", pattern, count])
        
        return results
    
    def is_subscribed(self) -> bool:
        """Check if client has any subscriptions."""
        return bool(self.subscribed_channels or self.subscribed_patterns)


def register_pubsub_commands(registry, pubsub_manager: PubSubManager):
    """Register pub/sub commands."""
    
    def cmd_publish(channel, message):
        """PUBLISH channel message - Publish to channel."""
        return pubsub_manager.publish(channel, message)
    
    def cmd_pubsub(subcommand, *args):
        """PUBSUB subcommand - Pub/sub introspection."""
        subcommand = subcommand.upper()
        
        if subcommand == "CHANNELS":
            pattern = args[0] if args else "*"
            return pubsub_manager.channels(pattern)
        elif subcommand == "NUMSUB":
            return pubsub_manager.numsub(*args)
        elif subcommand == "NUMPAT":
            return pubsub_manager.numpat()
        else:
            raise ValueError(f"ERR Unknown PUBSUB subcommand '{subcommand}'")
    
    registry.register("PUBLISH", cmd_publish, min_args=2, max_args=2)
    registry.register("PUBSUB", cmd_pubsub, min_args=1)
