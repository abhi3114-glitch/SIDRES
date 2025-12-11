"""
Server commands for Sedris.
"""

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import CommandRegistry
    from ..store import DataStore


def register_server_commands(registry: "CommandRegistry", store: "DataStore"):
    """Register server commands."""
    
    _server_info = {
        "start_time": time.time(),
        "commands_processed": 0,
    }
    
    def cmd_ping(*args):
        """PING [message] - Test connection."""
        if args:
            return args[0]
        return "PONG"
    
    def cmd_echo(message):
        """ECHO message - Echo message."""
        return message
    
    def cmd_dbsize():
        """DBSIZE - Get number of keys."""
        return store.dbsize()
    
    def cmd_flushdb(*args):
        """FLUSHDB [ASYNC] - Delete all keys."""
        store.flushdb()
        return "OK"
    
    def cmd_flushall(*args):
        """FLUSHALL [ASYNC] - Delete all keys in all databases."""
        store.flushdb()  # Single database for now
        return "OK"
    
    def cmd_info(*sections):
        """INFO [section] - Get server info."""
        import platform
        
        uptime = int(time.time() - _server_info["start_time"])
        
        info = f"""# Server
sedris_version:1.0.0
os:{platform.system()} {platform.release()}
arch_bits:64
process_id:{1}
uptime_in_seconds:{uptime}
uptime_in_days:{uptime // 86400}

# Clients
connected_clients:1
blocked_clients:0

# Memory
used_memory:{0}
used_memory_human:0B
used_memory_peak:{0}

# Stats
total_connections_received:1
total_commands_processed:{_server_info["commands_processed"]}
keyspace_hits:{0}
keyspace_misses:{0}

# Replication
role:master
connected_slaves:0

# Keyspace
db0:keys={store.dbsize()},expires={0}
"""
        _server_info["commands_processed"] += 1
        return info
    
    def cmd_time():
        """TIME - Get server time."""
        now = time.time()
        seconds = int(now)
        microseconds = int((now - seconds) * 1_000_000)
        return [str(seconds), str(microseconds)]
    
    def cmd_config(subcommand, *args):
        """CONFIG subcommand - Get/set configuration."""
        subcommand = subcommand.upper()
        
        if subcommand == "GET":
            if not args:
                return []
            pattern = args[0]
            # Return empty for now
            return []
        elif subcommand == "SET":
            return "OK"
        elif subcommand == "RESETSTAT":
            return "OK"
        elif subcommand == "REWRITE":
            return "OK"
        else:
            raise ValueError(f"ERR Unknown subcommand '{subcommand}'")
    
    def cmd_command(*args):
        """COMMAND - Get command info."""
        if not args:
            # Return all commands
            return registry.list_commands()
        
        subcommand = args[0].upper()
        if subcommand == "COUNT":
            return len(registry.list_commands())
        elif subcommand == "DOCS":
            return []
        elif subcommand == "INFO":
            return []
        elif subcommand == "LIST":
            return registry.list_commands()
        
        return []
    
    def cmd_client(*args):
        """CLIENT subcommand - Client management."""
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        subcommand = args[0].upper()
        
        if subcommand == "SETNAME":
            return "OK"
        elif subcommand == "GETNAME":
            return None
        elif subcommand == "LIST":
            return "id=1 addr=127.0.0.1:0 fd=1 name= age=0 idle=0"
        elif subcommand == "ID":
            return 1
        elif subcommand == "INFO":
            return "id=1 addr=127.0.0.1:0"
        elif subcommand == "KILL":
            return "OK"
        elif subcommand == "PAUSE":
            return "OK"
        elif subcommand == "UNPAUSE":
            return "OK"
        
        return "OK"
    
    def cmd_debug(*args):
        """DEBUG subcommand - Debug commands."""
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        subcommand = args[0].upper()
        
        if subcommand == "SLEEP":
            if len(args) > 1:
                time.sleep(float(args[1]))
            return "OK"
        
        return "OK"
    
    def cmd_memory(*args):
        """MEMORY subcommand - Memory commands."""
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        subcommand = args[0].upper()
        
        if subcommand == "USAGE":
            if len(args) < 2:
                raise ValueError("ERR wrong number of arguments")
            # Return estimated memory usage
            return 64  # Placeholder
        elif subcommand == "DOCTOR":
            return "Sam, I have no memory problems"
        elif subcommand == "STATS":
            return {"peak.allocated": 0, "total.allocated": 0}
        
        return None
    
    def cmd_select(index):
        """SELECT index - Select database."""
        # Single database for now
        idx = int(index)
        if idx < 0 or idx > 15:
            raise ValueError("ERR invalid DB index")
        return "OK"
    
    def cmd_lastsave():
        """LASTSAVE - Get last save timestamp."""
        return int(store.last_save)
    
    def cmd_quit():
        """QUIT - Close connection."""
        return "OK"
    
    def cmd_shutdown(*args):
        """SHUTDOWN - Shutdown server."""
        return "OK"
    
    def cmd_slowlog(*args):
        """SLOWLOG subcommand - Slow log."""
        if not args:
            raise ValueError("ERR wrong number of arguments")
        
        subcommand = args[0].upper()
        if subcommand == "GET":
            return []
        elif subcommand == "LEN":
            return 0
        elif subcommand == "RESET":
            return "OK"
        
        return []
    
    def cmd_acl(*args):
        """ACL subcommand - Access control."""
        if not args:
            return []
        
        subcommand = args[0].upper()
        if subcommand == "LIST":
            return ["user default on nopass ~* &* +@all"]
        elif subcommand == "WHOAMI":
            return "default"
        elif subcommand == "CAT":
            return []
        
        return []
    
    # Register commands
    registry.register("PING", cmd_ping, min_args=0, max_args=1)
    registry.register("ECHO", cmd_echo, min_args=1, max_args=1)
    registry.register("DBSIZE", cmd_dbsize, min_args=0, max_args=0)
    registry.register("FLUSHDB", cmd_flushdb, min_args=0)
    registry.register("FLUSHALL", cmd_flushall, min_args=0)
    registry.register("INFO", cmd_info, min_args=0)
    registry.register("TIME", cmd_time, min_args=0, max_args=0)
    registry.register("CONFIG", cmd_config, min_args=1)
    registry.register("COMMAND", cmd_command, min_args=0)
    registry.register("CLIENT", cmd_client, min_args=0)
    registry.register("DEBUG", cmd_debug, min_args=1)
    registry.register("MEMORY", cmd_memory, min_args=1)
    registry.register("SELECT", cmd_select, min_args=1, max_args=1)
    registry.register("LASTSAVE", cmd_lastsave, min_args=0, max_args=0)
    registry.register("QUIT", cmd_quit, min_args=0, max_args=0)
    registry.register("SHUTDOWN", cmd_shutdown, min_args=0)
    registry.register("SLOWLOG", cmd_slowlog, min_args=1)
    registry.register("ACL", cmd_acl, min_args=0)
