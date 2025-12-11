"""
Configuration management for Sedris server.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Server configuration settings."""
    
    # Network settings
    host: str = "127.0.0.1"
    port: int = 6379
    max_clients: int = 10000
    timeout: int = 0  # 0 = no timeout
    
    # Persistence settings
    data_dir: str = "./data"
    rdb_filename: str = "dump.rdb"
    save_intervals: list = field(default_factory=lambda: [
        (900, 1),    # Save after 900 sec if at least 1 key changed
        (300, 10),   # Save after 300 sec if at least 10 keys changed
        (60, 10000), # Save after 60 sec if at least 10000 keys changed
    ])
    
    # Memory settings
    maxmemory: int = 0  # 0 = no limit
    
    # Logging
    loglevel: str = "info"
    logfile: str = ""
    
    # Database settings
    databases: int = 16
    
    @classmethod
    def from_file(cls, filepath: str) -> "Config":
        """Load configuration from a file."""
        config = cls()
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split(None, 1)
                    if len(parts) == 2:
                        key, value = parts
                        key = key.lower()
                        if hasattr(config, key):
                            attr_type = type(getattr(config, key))
                            if attr_type == int:
                                setattr(config, key, int(value))
                            elif attr_type == str:
                                setattr(config, key, value.strip('"'))
        return config
    
    @property
    def rdb_path(self) -> str:
        """Full path to the RDB file."""
        return os.path.join(self.data_dir, self.rdb_filename)
