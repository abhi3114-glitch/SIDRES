"""
Sedris CLI - Interactive command-line client.

Usage: python -m sedris.cli [--host HOST] [--port PORT]
"""

import argparse
import socket
import sys
from typing import Optional, List


class SedrisCLI:
    """Interactive CLI client for Sedris."""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.connected = False
    
    def connect(self) -> bool:
        """Connect to Sedris server."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            return True
        except ConnectionRefusedError:
            print(f"Could not connect to Sedris at {self.host}:{self.port}")
            return False
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from server."""
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
        self.socket = None
        self.connected = False
    
    def send_command(self, command: List[str]) -> str:
        """Send command and receive response."""
        if not self.connected:
            return "Not connected"
        
        # Encode as RESP array
        request = f"*{len(command)}\r\n"
        for arg in command:
            arg_bytes = str(arg).encode('utf-8')
            request += f"${len(arg_bytes)}\r\n{arg}\r\n"
        
        try:
            self.socket.sendall(request.encode('utf-8'))
            
            # Receive response
            response = b""
            while True:
                chunk = self.socket.recv(4096)
                if not chunk:
                    break
                response += chunk
                # Simple check if response is complete
                if self._is_complete_response(response):
                    break
            
            return self._format_response(response.decode('utf-8'))
        
        except Exception as e:
            return f"Error: {e}"
    
    def _is_complete_response(self, data: bytes) -> bool:
        """Check if response is complete (simplified)."""
        if not data:
            return False
        
        # Check if ends with \r\n
        if not data.endswith(b"\r\n"):
            return False
        
        # Simple heuristic - could be improved
        return True
    
    def _format_response(self, response: str) -> str:
        """Format RESP response for display."""
        if not response:
            return "(empty)"
        
        lines = response.split('\r\n')
        if not lines:
            return "(empty)"
        
        first = lines[0]
        
        if first.startswith('+'):
            # Simple string
            return first[1:]
        elif first.startswith('-'):
            # Error
            return f"(error) {first[1:]}"
        elif first.startswith(':'):
            # Integer
            return f"(integer) {first[1:]}"
        elif first.startswith('$'):
            # Bulk string
            length = int(first[1:])
            if length == -1:
                return "(nil)"
            if len(lines) > 1:
                return f'"{lines[1]}"'
            return "(nil)"
        elif first.startswith('*'):
            # Array
            count = int(first[1:])
            if count == -1:
                return "(empty list or set)"
            if count == 0:
                return "(empty list or set)"
            
            return self._format_array(lines[1:], count)
        else:
            return response
    
    def _format_array(self, lines: List[str], count: int) -> str:
        """Format array response."""
        result = []
        i = 0
        item_num = 1
        
        while i < len(lines) and item_num <= count:
            line = lines[i]
            
            if line.startswith('$'):
                length = int(line[1:])
                if length == -1:
                    result.append(f"{item_num}) (nil)")
                else:
                    i += 1
                    if i < len(lines):
                        result.append(f'{item_num}) "{lines[i]}"')
                item_num += 1
            elif line.startswith(':'):
                result.append(f"{item_num}) (integer) {line[1:]}")
                item_num += 1
            elif line.startswith('+'):
                result.append(f"{item_num}) {line[1:]}")
                item_num += 1
            elif line.startswith('-'):
                result.append(f"{item_num}) (error) {line[1:]}")
                item_num += 1
            elif line == '':
                pass  # Skip empty lines
            
            i += 1
        
        return '\n'.join(result) if result else "(empty list or set)"
    
    def parse_input(self, line: str) -> List[str]:
        """Parse user input into command parts."""
        parts = []
        current = []
        in_quotes = False
        quote_char = None
        
        for char in line:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
            elif char == ' ' and not in_quotes:
                if current:
                    parts.append(''.join(current))
                    current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def run(self):
        """Run interactive CLI loop."""
        print(f"Connecting to {self.host}:{self.port}...")
        
        if not self.connect():
            return 1
        
        print("Connected!")
        print("Type 'quit' or 'exit' to close.\n")
        
        try:
            while True:
                try:
                    prompt = f"{self.host}:{self.port}> "
                    line = input(prompt).strip()
                    
                    if not line:
                        continue
                    
                    if line.lower() in ('quit', 'exit'):
                        break
                    
                    if line.lower() == 'clear':
                        print('\033[2J\033[H', end='')
                        continue
                    
                    if line.lower() == 'help':
                        self._print_help()
                        continue
                    
                    command = self.parse_input(line)
                    if command:
                        response = self.send_command(command)
                        print(response)
                
                except KeyboardInterrupt:
                    print("\nUse 'quit' to exit")
                except EOFError:
                    break
        
        finally:
            self.disconnect()
            print("Goodbye!")
        
        return 0
    
    def _print_help(self):
        """Print help message."""
        print("""
Sedris CLI Help
===============
Enter Redis commands directly, for example:
  SET foo bar
  GET foo
  LPUSH mylist item1 item2
  KEYS *

Special commands:
  quit, exit - Close the connection
  clear      - Clear the screen
  help       - Show this help message
""")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Sedris CLI")
    parser.add_argument("--host", type=str, default="127.0.0.1",
                        help="Server host")
    parser.add_argument("--port", "-p", type=int, default=6379,
                        help="Server port")
    
    args = parser.parse_args()
    
    cli = SedrisCLI(host=args.host, port=args.port)
    return cli.run()


if __name__ == "__main__":
    sys.exit(main())
