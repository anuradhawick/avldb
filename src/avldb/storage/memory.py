"""Array-backed in-memory reference backend."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping

from ..contracts import ChangeSet, IndexLookup, IndexSpec
from ..exceptions import BackendError, DuplicateKeyError, WriteConflictError
from ..indexing.avl import Index
from .base import BackendView, DocumentData, StorageBackend, _copy_document, _lookup_index

@dataclass(frozen=True, slots=True)
class _MemoryState:
    """Copy-on-write records and indexes captured by an in-memory view."""

    revision: int
    records: tuple[DocumentData | None, ...]
    locations: Mapping[str, int]
    specs: Mapping[str, IndexSpec]
    indexes: Mapping[str, Index]


class _MemoryView(BackendView):
    """Read-only view over one copy-on-write memory state."""

    def __init__(self, state: _MemoryState) -> None:
        """Capture a state whose objects will never be mutated in place."""

        self._state = state

    @property
    def revision(self) -> str:
        """Return the state's integer revision as a token string."""

        return str(self._state.revision)

    @property
    def indexes(self) -> Mapping[str, IndexSpec]:
        """Return immutable index specifications for this state."""

        return self._state.specs

    def scan(self) -> Iterator[DocumentData]:
        """Yield copied live entries from the record array."""

        for document in self._state.records:
            if document is not None:
                yield _copy_document(document)

    def lookup(self, field: str, lookup: IndexLookup) -> set[str]:
        """Execute an index lookup against the state's cached AVL tree."""

        try:
            index = self._state.indexes[field]
        except KeyError as error:
            raise BackendError(f"index does not exist: {field!r}") from error
        return _lookup_index(index, lookup)

    def fetch(self, document_ids: Iterable[str]) -> Iterator[DocumentData]:
        """Resolve IDs to record-array slots and yield copied live documents."""

        for document_id in document_ids:
            slot = self._state.locations.get(document_id)
            if slot is not None:
                document = self._state.records[slot]
                if document is not None:
                    yield _copy_document(document)

    def close(self) -> None:
        """Close a memory view; immutable state needs no explicit release."""


class MemoryBackend(StorageBackend):
    """Array-backed reference backend with copy-on-write AVL indexes."""

    def __init__(self) -> None:
        """Create a closed backend with an empty initial state."""

        specs = {"_id": IndexSpec("_id", unique=True)}
        self._state = _MemoryState(0, (), MappingProxyType({}), MappingProxyType(specs), MappingProxyType({"_id": Index(specs["_id"])}))
        self._opened = False
        self._closed = False
        self._lock = threading.RLock()

    def _check_open(self) -> None:
        """Reject operations before opening or after closing."""

        if not self._opened or self._closed:
            raise BackendError("memory backend is not open")

    def open(self) -> None:
        """Mark the backend open; no external resources are required."""

        with self._lock:
            if self._closed:
                raise BackendError("memory backend is closed")
            self._opened = True

    def view(self) -> BackendView:
        """Return a stable view by capturing the current copy-on-write state."""

        with self._lock:
            self._check_open()
            return _MemoryView(self._state)

    def commit(self, base_revision: str, changes: ChangeSet) -> str:
        """Build replacement arrays/indexes and atomically swap memory state."""

        with self._lock:
            self._check_open()
            if base_revision != str(self._state.revision):
                raise WriteConflictError("memory backend revision changed")
            if changes.empty:
                return base_revision

            if any(not isinstance(document.get("_id"), str) or not document.get("_id") for document in changes.puts):
                raise BackendError("stored documents require a non-empty string _id")
            put_ids = [str(document["_id"]) for document in changes.puts]
            if len(set(put_ids)) != len(put_ids):
                duplicate = next(item for item in put_ids if put_ids.count(item) > 1)
                raise DuplicateKeyError("_id", duplicate)

            records = list(self._state.records)
            locations = dict(self._state.locations)
            for document_id in changes.deletes:
                slot = locations.pop(document_id, None)
                if slot is not None:
                    records[slot] = None
            for raw_document in changes.puts:
                document = _copy_document(raw_document)
                document_id = document.get("_id")
                if not isinstance(document_id, str):
                    raise BackendError("stored documents require a string _id")
                old_slot = locations.get(document_id)
                if old_slot is not None:
                    records[old_slot] = None
                locations[document_id] = len(records)
                records.append(document)

            specs = dict(self._state.specs)
            for field in changes.drop_indexes:
                if field == "_id":
                    raise BackendError("the _id index cannot be removed")
                specs.pop(field, None)
            for spec in changes.create_indexes:
                existing = specs.get(spec.field)
                if existing is not None and existing != spec:
                    raise BackendError(f"index already exists with different options: {spec.field!r}")
                specs[spec.field] = spec

            live = [document for document in records if document is not None]
            indexes = {field: Index.build(spec, live) for field, spec in specs.items()}
            revision = self._state.revision + 1
            self._state = _MemoryState(
                revision,
                tuple(records),
                MappingProxyType(locations),
                MappingProxyType(specs),
                MappingProxyType(indexes),
            )
            return str(revision)

    def compact(self) -> str:
        """Remove tombstoned array slots while preserving documents and indexes."""

        with self._lock:
            self._check_open()
            live = tuple(_copy_document(document) for document in self._state.records if document is not None)
            locations = {str(document["_id"]): slot for slot, document in enumerate(live)}
            indexes = {field: Index.build(spec, live) for field, spec in self._state.specs.items()}
            revision = self._state.revision + 1
            self._state = _MemoryState(
                revision,
                live,
                MappingProxyType(locations),
                self._state.specs,
                MappingProxyType(indexes),
            )
            return str(revision)

    def close(self) -> None:
        """Mark the backend closed; previously created views remain readable."""

        with self._lock:
            self._closed = True
