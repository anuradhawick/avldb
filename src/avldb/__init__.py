"""Typed embedded document database with pluggable storage and indexes."""

from .contracts import Bound, ChangeSet, IndexLookup, IndexSpec, UpdateResult
from .core.collection import Collection, Cursor
from .core.document import Document
from .exceptions import (
    AvlDBError,
    BackendError,
    ClosedDatabaseError,
    CorruptDataError,
    DatabaseLockedError,
    DuplicateKeyError,
    QueryError,
    ValidationError,
    WriteConflictError,
)
from .storage import BackendView, DiskBackend, MemoryBackend, StorageBackend

__all__ = [
    "AvlDBError",
    "BackendError",
    "BackendView",
    "Bound",
    "ChangeSet",
    "ClosedDatabaseError",
    "Collection",
    "CorruptDataError",
    "Cursor",
    "DatabaseLockedError",
    "DiskBackend",
    "Document",
    "DuplicateKeyError",
    "IndexSpec",
    "IndexLookup",
    "MemoryBackend",
    "QueryError",
    "StorageBackend",
    "UpdateResult",
    "ValidationError",
    "WriteConflictError",
]

__version__ = "0.1.2"
