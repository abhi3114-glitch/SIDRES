"""
RESP (Redis Serialization Protocol) parser and encoder.

Supports RESP2 protocol:
- Simple Strings: +OK\r\n
- Errors: -ERR message\r\n
- Integers: :1000\r\n
- Bulk Strings: $6\r\nfoobar\r\n
- Arrays: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
- Null Bulk String: $-1\r\n
- Null Array: *-1\r\n
"""

from typing import Any, List, Optional, Tuple, Union

CRLF = b"\r\n"


class ProtocolError(Exception):
    """Exception raised for RESP protocol errors."""
    pass


class RESPParser:
    """Parser for RESP protocol messages."""
    
    def __init__(self):
        self.buffer = b""
    
    def feed(self, data: bytes) -> None:
        """Add data to the buffer."""
        self.buffer += data
    
    def parse_one(self) -> Tuple[Optional[Any], int]:
        """
        Parse one complete RESP message from the buffer.
        Returns (parsed_value, bytes_consumed) or (None, 0) if incomplete.
        """
        if not self.buffer:
            return None, 0
        
        return self._parse(self.buffer)
    
    def get_message(self) -> Optional[Any]:
        """
        Try to parse and return one complete message.
        Removes consumed bytes from the buffer.
        """
        result, consumed = self.parse_one()
        if consumed > 0:
            self.buffer = self.buffer[consumed:]
            return result
        return None
    
    def _parse(self, data: bytes) -> Tuple[Optional[Any], int]:
        """Parse RESP data and return (value, bytes_consumed)."""
        if len(data) < 3:  # Minimum: type + \r\n
            return None, 0
        
        type_byte = chr(data[0])
        
        if type_byte == '+':
            return self._parse_simple_string(data)
        elif type_byte == '-':
            return self._parse_error(data)
        elif type_byte == ':':
            return self._parse_integer(data)
        elif type_byte == '$':
            return self._parse_bulk_string(data)
        elif type_byte == '*':
            return self._parse_array(data)
        else:
            # Inline command (plain text)
            return self._parse_inline(data)
    
    def _find_crlf(self, data: bytes, start: int = 0) -> int:
        """Find CRLF position, return -1 if not found."""
        pos = data.find(CRLF, start)
        return pos
    
    def _parse_simple_string(self, data: bytes) -> Tuple[Optional[str], int]:
        """Parse simple string: +OK\r\n"""
        crlf_pos = self._find_crlf(data)
        if crlf_pos == -1:
            return None, 0
        value = data[1:crlf_pos].decode('utf-8')
        return value, crlf_pos + 2
    
    def _parse_error(self, data: bytes) -> Tuple[Optional[Exception], int]:
        """Parse error: -ERR message\r\n"""
        crlf_pos = self._find_crlf(data)
        if crlf_pos == -1:
            return None, 0
        message = data[1:crlf_pos].decode('utf-8')
        return ProtocolError(message), crlf_pos + 2
    
    def _parse_integer(self, data: bytes) -> Tuple[Optional[int], int]:
        """Parse integer: :1000\r\n"""
        crlf_pos = self._find_crlf(data)
        if crlf_pos == -1:
            return None, 0
        value = int(data[1:crlf_pos].decode('utf-8'))
        return value, crlf_pos + 2
    
    def _parse_bulk_string(self, data: bytes) -> Tuple[Optional[Union[str, None]], int]:
        """Parse bulk string: $6\r\nfoobar\r\n or null: $-1\r\n"""
        crlf_pos = self._find_crlf(data)
        if crlf_pos == -1:
            return None, 0
        
        length = int(data[1:crlf_pos].decode('utf-8'))
        
        # Null bulk string
        if length == -1:
            return None, crlf_pos + 2
        
        # Check if we have the full string
        content_start = crlf_pos + 2
        content_end = content_start + length
        
        if len(data) < content_end + 2:
            return None, 0
        
        # Verify trailing CRLF
        if data[content_end:content_end + 2] != CRLF:
            raise ProtocolError("Missing CRLF after bulk string")
        
        value = data[content_start:content_end].decode('utf-8')
        return value, content_end + 2
    
    def _parse_array(self, data: bytes) -> Tuple[Optional[List], int]:
        """Parse array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"""
        crlf_pos = self._find_crlf(data)
        if crlf_pos == -1:
            return None, 0
        
        count = int(data[1:crlf_pos].decode('utf-8'))
        
        # Null array
        if count == -1:
            return None, crlf_pos + 2
        
        elements = []
        offset = crlf_pos + 2
        
        for _ in range(count):
            if offset >= len(data):
                return None, 0
            
            element, consumed = self._parse(data[offset:])
            if consumed == 0:
                return None, 0
            
            elements.append(element)
            offset += consumed
        
        return elements, offset
    
    def _parse_inline(self, data: bytes) -> Tuple[Optional[List[str]], int]:
        """Parse inline command: PING\r\n or SET foo bar\r\n"""
        crlf_pos = self._find_crlf(data)
        if crlf_pos == -1:
            return None, 0
        
        line = data[:crlf_pos].decode('utf-8').strip()
        if not line:
            return None, 0
        
        # Split by whitespace, respecting quotes
        parts = self._split_inline(line)
        return parts, crlf_pos + 2
    
    def _split_inline(self, line: str) -> List[str]:
        """Split inline command respecting quotes."""
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


class RESPEncoder:
    """Encoder for RESP protocol responses."""
    
    @staticmethod
    def encode(value: Any) -> bytes:
        """Encode a Python value to RESP format."""
        if value is None:
            return b"$-1\r\n"
        elif isinstance(value, bool):
            return RESPEncoder.encode_integer(1 if value else 0)
        elif isinstance(value, int):
            return RESPEncoder.encode_integer(value)
        elif isinstance(value, str):
            return RESPEncoder.encode_bulk_string(value)
        elif isinstance(value, bytes):
            return RESPEncoder.encode_bulk_bytes(value)
        elif isinstance(value, (list, tuple)):
            return RESPEncoder.encode_array(value)
        elif isinstance(value, Exception):
            return RESPEncoder.encode_error(str(value))
        elif isinstance(value, dict):
            # Encode dict as flat array [k1, v1, k2, v2, ...]
            flat = []
            for k, v in value.items():
                flat.extend([str(k), str(v) if not isinstance(v, (str, bytes)) else v])
            return RESPEncoder.encode_array(flat)
        else:
            return RESPEncoder.encode_bulk_string(str(value))
    
    @staticmethod
    def encode_simple_string(value: str) -> bytes:
        """Encode simple string: +OK\r\n"""
        return f"+{value}\r\n".encode('utf-8')
    
    @staticmethod
    def encode_error(message: str) -> bytes:
        """Encode error: -ERR message\r\n"""
        if not message.startswith(('ERR', 'WRONGTYPE')):
            message = f"ERR {message}"
        return f"-{message}\r\n".encode('utf-8')
    
    @staticmethod
    def encode_integer(value: int) -> bytes:
        """Encode integer: :1000\r\n"""
        return f":{value}\r\n".encode('utf-8')
    
    @staticmethod
    def encode_bulk_string(value: str) -> bytes:
        """Encode bulk string: $6\r\nfoobar\r\n"""
        encoded = value.encode('utf-8')
        return f"${len(encoded)}\r\n".encode('utf-8') + encoded + CRLF
    
    @staticmethod
    def encode_bulk_bytes(value: bytes) -> bytes:
        """Encode bulk bytes."""
        return f"${len(value)}\r\n".encode('utf-8') + value + CRLF
    
    @staticmethod
    def encode_array(values: List[Any]) -> bytes:
        """Encode array: *2\r\n..."""
        if values is None:
            return b"*-1\r\n"
        
        result = f"*{len(values)}\r\n".encode('utf-8')
        for value in values:
            result += RESPEncoder.encode(value)
        return result
    
    @staticmethod
    def ok() -> bytes:
        """Return OK response."""
        return b"+OK\r\n"
    
    @staticmethod
    def pong() -> bytes:
        """Return PONG response."""
        return b"+PONG\r\n"
    
    @staticmethod
    def null() -> bytes:
        """Return null bulk string."""
        return b"$-1\r\n"
    
    @staticmethod
    def null_array() -> bytes:
        """Return null array."""
        return b"*-1\r\n"
