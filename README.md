# Sedris

A high-performance, Redis-compatible in-memory data store built entirely in Python. Sedris implements the Redis Serialization Protocol (RESP) and supports all major Redis data types and commands.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Supported Commands](#supported-commands)
- [Data Types](#data-types)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [API Reference](#api-reference)
- [Limitations](#limitations)
- [License](#license)

## Features

- Full RESP (Redis Serialization Protocol) support
- Compatible with redis-cli and Redis client libraries
- 129 commands implemented across all data types
- Five core data types: Strings, Lists, Sets, Hashes, Sorted Sets
- Key expiration with TTL support (active and passive cleanup)
- RDB-style persistence with background saving
- Publish/Subscribe messaging system
- Built-in CLI client
- Zero external dependencies (pure Python standard library)
- Asyncio-based TCP server for concurrent client handling
- Thread-safe data store implementation

## Installation

### Prerequisites

- Python 3.8 or higher

### Clone the Repository

```bash
git clone https://github.com/yourusername/sedris.git
cd sedris
```

### No Additional Dependencies Required

Sedris uses only Python standard library modules. No pip install is necessary.

## Quick Start

### Starting the Server

```bash
# Start with default settings (port 6379)
python -m sedris

# Start with custom port
python -m sedris --port 6380

# Start with debug logging
python -m sedris --loglevel debug
```

### Connecting with redis-cli

```bash
redis-cli -p 6379

127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET mykey "Hello World"
OK
127.0.0.1:6379> GET mykey
"Hello World"
```

### Using the Built-in CLI

```bash
python -m sedris.cli

127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET foo bar
OK
127.0.0.1:6379> GET foo
"bar"
```

## Usage

### Basic String Operations

```bash
SET key value           # Set a key
GET key                 # Get a key
INCR counter            # Increment integer value
APPEND key " more"      # Append to string
STRLEN key              # Get string length
```

### List Operations

```bash
LPUSH mylist a b c      # Push to head
RPUSH mylist x y z      # Push to tail
LRANGE mylist 0 -1      # Get all elements
LPOP mylist             # Pop from head
RPOP mylist             # Pop from tail
LLEN mylist             # Get list length
```

### Set Operations

```bash
SADD myset a b c        # Add members
SMEMBERS myset          # Get all members
SISMEMBER myset a       # Check membership
SINTER set1 set2        # Intersection
SUNION set1 set2        # Union
SDIFF set1 set2         # Difference
```

### Hash Operations

```bash
HSET user name "John" age 30    # Set fields
HGET user name                   # Get field
HGETALL user                     # Get all fields
HDEL user age                    # Delete field
HINCRBY user age 1               # Increment field
```

### Sorted Set Operations

```bash
ZADD scores 100 alice 85 bob    # Add with scores
ZRANGE scores 0 -1 WITHSCORES   # Get range
ZRANK scores alice              # Get rank
ZSCORE scores alice             # Get score
ZINCRBY scores 10 alice         # Increment score
```

### Key Management

```bash
KEYS *                  # List all keys
EXISTS key              # Check if key exists
DEL key                 # Delete key
TYPE key                # Get key type
RENAME old new          # Rename key
EXPIRE key 60           # Set TTL in seconds
TTL key                 # Get remaining TTL
PERSIST key             # Remove expiration
```

### Server Commands

```bash
PING                    # Test connection
INFO                    # Server information
DBSIZE                  # Number of keys
FLUSHDB                 # Delete all keys
SELECT 0                # Select database
TIME                    # Server time
```

## Supported Commands

### String Commands (22)

GET, SET, SETNX, SETEX, PSETEX, MGET, MSET, MSETNX, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, APPEND, STRLEN, GETRANGE, SETRANGE, GETSET, GETDEL, GETEX, SUBSTR

### List Commands (20)

LPUSH, LPUSHX, RPUSH, RPUSHX, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, RPOPLPUSH, LMOVE, LPOS, BLPOP, BRPOP, BRPOPLPUSH, BLMOVE

### Set Commands (17)

SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SMOVE, SUNION, SUNIONSTORE, SINTER, SINTERSTORE, SINTERCARD, SDIFF, SDIFFSTORE, SSCAN

### Hash Commands (16)

HSET, HSETNX, HGET, HMSET, HMGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL, HINCRBY, HINCRBYFLOAT, HSTRLEN, HRANDFIELD, HSCAN

### Sorted Set Commands (19)

ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZCARD, ZCOUNT, ZINCRBY, ZPOPMIN, ZPOPMAX, ZMSCORE, ZRANGESTORE, ZSCAN, BZPOPMIN, BZPOPMAX

### Key Commands (17)

DEL, EXISTS, KEYS, TYPE, RENAME, RENAMENX, EXPIRE, EXPIREAT, PEXPIRE, TTL, PTTL, PERSIST, RANDOMKEY, SCAN, TOUCH, UNLINK, DUMP, OBJECT

### Server Commands (18)

PING, ECHO, DBSIZE, FLUSHDB, FLUSHALL, INFO, TIME, CONFIG, COMMAND, CLIENT, DEBUG, MEMORY, SELECT, LASTSAVE, QUIT, SHUTDOWN, SLOWLOG, ACL

### Pub/Sub Commands (2)

PUBLISH, PUBSUB

## Data Types

### Strings

The most basic data type. Can hold any binary data up to 512MB. Supports atomic increment/decrement operations for integer values.

### Lists

Ordered collections of strings implemented as doubly-linked lists. Provides O(1) push/pop operations on both ends.

### Sets

Unordered collections of unique strings. Supports set operations like union, intersection, and difference.

### Hashes

Maps between string fields and string values. Ideal for representing objects with multiple attributes.

### Sorted Sets

Sets where each member has an associated score. Members are ordered by score, allowing range queries and ranking operations.

## Configuration

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| --port, -p | 6379 | Port to listen on |
| --host | 127.0.0.1 | Host address to bind |
| --loglevel, -l | info | Log level (debug, info, warning, error) |

### Example

```bash
python -m sedris --port 6380 --host 0.0.0.0 --loglevel debug
```

## Architecture

```
sedris/
├── __init__.py          # Package initialization
├── __main__.py          # Module entry point
├── server.py            # Asyncio TCP server
├── protocol.py          # RESP parser and encoder
├── store.py             # In-memory data store
├── config.py            # Configuration management
├── persistence.py       # RDB persistence
├── pubsub.py            # Publish/Subscribe system
├── cli.py               # Command-line client
├── commands/            # Command implementations
│   ├── __init__.py      # Command registry
│   ├── keys.py          # Key management commands
│   ├── server_commands.py
│   ├── string_commands.py
│   ├── list_commands.py
│   ├── set_commands.py
│   ├── hash_commands.py
│   └── zset_commands.py
└── datatypes/           # Data type handlers
    ├── __init__.py
    ├── strings.py
    ├── lists.py
    ├── sets.py
    ├── hashes.py
    └── sorted_sets.py
```

### Component Overview

**Server (server.py)**
Asyncio-based TCP server that handles multiple concurrent client connections. Parses incoming RESP messages and routes commands to appropriate handlers.

**Protocol (protocol.py)**
Implements the Redis Serialization Protocol (RESP). Handles parsing of client requests and encoding of server responses.

**Store (store.py)**
Thread-safe in-memory data store. Manages key-value storage, type tracking, and TTL expiration.

**Commands (commands/)**
Command handlers organized by data type. Each module registers its commands with the central command registry.

**Data Types (datatypes/)**
Type-specific operation handlers that implement Redis semantics for each data type.

**Persistence (persistence.py)**
RDB-style persistence with background saving. Serializes and restores data store state.

**Pub/Sub (pubsub.py)**
Publish/Subscribe messaging system with channel and pattern subscriptions.

## API Reference

### Connecting via Python

```python
import socket

def send_command(sock, *args):
    """Send a RESP command and receive response."""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_bytes = str(arg).encode()
        cmd += f"${len(arg_bytes)}\r\n{arg}\r\n"
    sock.sendall(cmd.encode())
    return sock.recv(4096).decode()

# Connect to Sedris
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('127.0.0.1', 6379))

# Send commands
print(send_command(sock, 'PING'))           # +PONG
print(send_command(sock, 'SET', 'key', 'value'))  # +OK
print(send_command(sock, 'GET', 'key'))     # $5\r\nvalue

sock.close()
```

### Using with Redis Python Client

```python
import redis

# Connect to Sedris (same as connecting to Redis)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Use standard Redis operations
r.set('foo', 'bar')
print(r.get('foo'))  # 'bar'

r.lpush('mylist', 'a', 'b', 'c')
print(r.lrange('mylist', 0, -1))  # ['c', 'b', 'a']
```

## Limitations

Sedris is designed for development, testing, and educational purposes. For production workloads, consider the following limitations:

### Performance
- Written in Python, which is slower than Redis (written in C)
- Suitable for low to moderate traffic applications

### Missing Features
- No clustering support
- No replication/master-slave setup
- No Lua scripting
- No MULTI/EXEC transactions (basic support only)
- Blocking commands are non-blocking implementations

### Recommended Use Cases
- Local development and testing
- Educational purposes and learning Redis internals
- Prototyping and proof-of-concept projects
- Embedded use within Python applications
- Environments where installing Redis is not feasible

### Not Recommended For
- High-traffic production systems
- Applications requiring clustering or replication
- Performance-critical applications

## Performance Characteristics

| Operation | Complexity |
|-----------|------------|
| GET/SET | O(1) |
| INCR/DECR | O(1) |
| LPUSH/RPUSH/LPOP/RPOP | O(1) |
| LRANGE | O(N) |
| SADD/SREM/SISMEMBER | O(1) |
| HGET/HSET | O(1) |
| HGETALL | O(N) |
| ZADD/ZREM | O(log N) |
| ZRANGE | O(log N + M) |

## Contributing

Contributions are welcome. Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License

Copyright (c) 2024

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Acknowledgments

- Redis for the protocol specification and command semantics
- The Python asyncio library for providing an excellent async I/O framework
