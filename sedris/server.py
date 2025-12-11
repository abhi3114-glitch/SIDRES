"""
Sedris TCP Server implementation using asyncio.

Handles client connections and routes commands through the protocol parser.
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

from .config import Config
from .store import DataStore
from .protocol import RESPParser, RESPEncoder
from .commands import CommandHandler

logger = logging.getLogger("sedris")


class ClientConnection:
    """Handles a single client connection."""
    
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 command_handler: CommandHandler, client_id: int):
        self.reader = reader
        self.writer = writer
        self.command_handler = command_handler
        self.client_id = client_id
        self.parser = RESPParser()
        self.addr = writer.get_extra_info('peername')
        self.closed = False
    
    async def handle(self):
        """Handle client request/response loop."""
        logger.info(f"Client {self.client_id} connected from {self.addr}")
        
        try:
            while not self.closed:
                # Read data from client
                data = await self.reader.read(4096)
                if not data:
                    break
                
                # Feed data to parser
                self.parser.feed(data)
                
                # Process all complete messages
                while True:
                    command = self.parser.get_message()
                    if command is None:
                        break
                    
                    # Handle QUIT command specially
                    if isinstance(command, list) and command and command[0].upper() == "QUIT":
                        self.writer.write(RESPEncoder.ok())
                        await self.writer.drain()
                        self.closed = True
                        break
                    
                    # Execute command and send response
                    response = self.command_handler.execute(command)
                    self.writer.write(response)
                    await self.writer.drain()
        
        except asyncio.CancelledError:
            pass
        except ConnectionResetError:
            pass
        except Exception as e:
            logger.error(f"Client {self.client_id} error: {e}")
        finally:
            await self.close()
    
    async def close(self):
        """Close the connection."""
        if not self.closed:
            self.closed = True
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            logger.info(f"Client {self.client_id} disconnected")


class SedrisServer:
    """Main Sedris server class."""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.store = DataStore()
        self.command_handler = CommandHandler(self.store)
        self.server: Optional[asyncio.Server] = None
        self.clients: dict = {}
        self.client_counter = 0
        self.running = False
        self._shutdown_event: Optional[asyncio.Event] = None
    
    async def start(self):
        """Start the server."""
        self.running = True
        self._shutdown_event = asyncio.Event()
        
        # Create TCP server
        self.server = await asyncio.start_server(
            self._handle_client,
            self.config.host,
            self.config.port,
            reuse_address=True
        )
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"Sedris server listening on {addr[0]}:{addr[1]}")
        
        # Print startup banner
        print(self._get_banner())
        print(f"Ready to accept connections on port {self.config.port}")
        
        try:
            async with self.server:
                await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
    
    async def _handle_client(self, reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter):
        """Handle new client connection."""
        self.client_counter += 1
        client_id = self.client_counter
        
        client = ClientConnection(reader, writer, self.command_handler, client_id)
        self.clients[client_id] = client
        
        try:
            await client.handle()
        finally:
            self.clients.pop(client_id, None)
    
    async def stop(self):
        """Stop the server gracefully."""
        logger.info("Shutting down server...")
        self.running = False
        
        # Close all client connections
        for client in list(self.clients.values()):
            await client.close()
        
        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        logger.info("Server stopped")
    
    def shutdown(self):
        """Signal server to shutdown."""
        if self._shutdown_event:
            self._shutdown_event.set()
    
    def _get_banner(self) -> str:
        """Get startup banner."""
        return f"""
   _____          _      _     
  / ____|        | |    (_)    
 | (___   ___  __| |_ __ _ ___ 
  \\___ \\ / _ \\/ _` | '__| / __|
  ____) |  __/ (_| | |  | \\__ \\
 |_____/ \\___|\\__,_|_|  |_|___/
                               
 Sedris v1.0.0 - Redis-compatible server
 Port: {self.config.port} | PID: {self._get_pid()}
"""
    
    def _get_pid(self) -> int:
        """Get process ID."""
        import os
        return os.getpid()


def setup_logging(level: str = "info"):
    """Setup logging configuration."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    
    logger.setLevel(log_level)
    logger.addHandler(handler)


async def run_server(config: Optional[Config] = None):
    """Run the Sedris server."""
    server = SedrisServer(config)
    
    # Setup signal handlers
    loop = asyncio.get_running_loop()
    
    def signal_handler():
        server.shutdown()
    
    # Windows doesn't support add_signal_handler for SIGTERM
    if sys.platform != 'win32':
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
    
    await server.start()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Sedris - Redis-compatible server")
    parser.add_argument("--port", "-p", type=int, default=6379, help="Port to listen on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--loglevel", "-l", type=str, default="info",
                        choices=["debug", "info", "warning", "error"],
                        help="Log level")
    
    args = parser.parse_args()
    
    config = Config(host=args.host, port=args.port, loglevel=args.loglevel)
    setup_logging(config.loglevel)
    
    try:
        asyncio.run(run_server(config))
    except KeyboardInterrupt:
        print("\nShutting down...")


if __name__ == "__main__":
    main()
