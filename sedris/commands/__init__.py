"""
Command registry and dispatcher for Sedris.
"""

from typing import Callable, Dict, List, Tuple, Any, TYPE_CHECKING
from ..protocol import RESPEncoder

if TYPE_CHECKING:
    from ..store import DataStore


class CommandError(Exception):
    """Error raised when command execution fails."""
    pass


class CommandRegistry:
    """Registry for command handlers."""
    
    def __init__(self):
        self._commands: Dict[str, Tuple[Callable, int, int, str]] = {}
        # command_name -> (handler, min_args, max_args, description)
    
    def register(self, name: str, handler: Callable,
                 min_args: int = 0, max_args: int = -1,
                 description: str = ""):
        """Register a command handler."""
        self._commands[name.upper()] = (handler, min_args, max_args, description)
    
    def get(self, name: str) -> Tuple[Callable, int, int, str]:
        """Get command handler and metadata."""
        return self._commands.get(name.upper())
    
    def exists(self, name: str) -> bool:
        """Check if command exists."""
        return name.upper() in self._commands
    
    def list_commands(self) -> List[str]:
        """List all registered commands."""
        return list(self._commands.keys())


class CommandHandler:
    """Handles command execution and dispatching."""
    
    def __init__(self, store: "DataStore"):
        from .keys import register_key_commands
        from .server_commands import register_server_commands
        from .string_commands import register_string_commands
        from .list_commands import register_list_commands
        from .set_commands import register_set_commands
        from .hash_commands import register_hash_commands
        from .zset_commands import register_zset_commands
        
        self.store = store
        self.registry = CommandRegistry()
        
        # Register all commands
        register_key_commands(self.registry, store)
        register_server_commands(self.registry, store)
        register_string_commands(self.registry, store)
        register_list_commands(self.registry, store)
        register_set_commands(self.registry, store)
        register_hash_commands(self.registry, store)
        register_zset_commands(self.registry, store)
    
    def execute(self, command: List[str]) -> bytes:
        """Execute a command and return RESP-encoded response."""
        if not command:
            return RESPEncoder.encode_error("ERR empty command")
        
        cmd_name = command[0].upper()
        args = command[1:]
        
        cmd_info = self.registry.get(cmd_name)
        if not cmd_info:
            return RESPEncoder.encode_error(f"ERR unknown command '{cmd_name}'")
        
        handler, min_args, max_args, _ = cmd_info
        
        # Validate argument count
        if len(args) < min_args:
            return RESPEncoder.encode_error(
                f"ERR wrong number of arguments for '{cmd_name}' command"
            )
        if max_args >= 0 and len(args) > max_args:
            return RESPEncoder.encode_error(
                f"ERR wrong number of arguments for '{cmd_name}' command"
            )
        
        try:
            result = handler(*args)
            return self._encode_result(result)
        except TypeError as e:
            if "WRONGTYPE" in str(e):
                return RESPEncoder.encode_error(str(e))
            return RESPEncoder.encode_error(f"ERR {e}")
        except ValueError as e:
            return RESPEncoder.encode_error(str(e))
        except Exception as e:
            return RESPEncoder.encode_error(f"ERR {e}")
    
    def _encode_result(self, result: Any) -> bytes:
        """Encode command result to RESP format."""
        if result is None:
            return RESPEncoder.null()
        elif isinstance(result, bool):
            return RESPEncoder.encode_integer(1 if result else 0)
        elif isinstance(result, int):
            return RESPEncoder.encode_integer(result)
        elif isinstance(result, float):
            return RESPEncoder.encode_bulk_string(str(result))
        elif isinstance(result, str):
            if result == "OK":
                return RESPEncoder.ok()
            elif result == "PONG":
                return RESPEncoder.pong()
            elif result == "QUEUED":
                return RESPEncoder.encode_simple_string("QUEUED")
            return RESPEncoder.encode_bulk_string(result)
        elif isinstance(result, (list, tuple)):
            return RESPEncoder.encode_array(result)
        elif isinstance(result, dict):
            # Flatten dict to array for RESP
            flat = []
            for k, v in result.items():
                flat.append(str(k))
                flat.append(str(v) if v is not None else None)
            return RESPEncoder.encode_array(flat)
        else:
            return RESPEncoder.encode_bulk_string(str(result))
