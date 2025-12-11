"""
Sedris - A Redis-compatible in-memory data store.

A high-performance key-value store implementing the Redis protocol.
"""

__version__ = "1.0.0"
__author__ = "Sedris Team"

from .server import SedrisServer
from .store import DataStore
from .config import Config

__all__ = ["SedrisServer", "DataStore", "Config", "__version__"]
