"""Extensible storage contracts and bundled backend implementations."""

from .base import BackendView, StorageBackend
from .disk import DiskBackend
from .memory import MemoryBackend

__all__ = ["BackendView", "DiskBackend", "MemoryBackend", "StorageBackend"]
