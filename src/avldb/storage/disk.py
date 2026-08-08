"""Directory backend with immutable content/index segments and a manifest."""

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping

from ..contracts import ChangeSet, IndexLookup, IndexSpec
from ..exceptions import (
    BackendError,
    CorruptDataError,
    DatabaseLockedError,
    DuplicateKeyError,
    WriteConflictError,
)
from ..indexing.avl import Index
from .base import (
    BackendView,
    DocumentData,
    StorageBackend,
    _copy_document,
    _lookup_index,
)
from .codec import _decode_json, _encode_json


@dataclass(frozen=True, slots=True)
class _Location:
    """Byte location of one put record inside a content segment."""

    segment: str
    offset: int
    length: int


@dataclass(frozen=True, slots=True)
class _DiskState:
    """Manifest-derived indexes and record locations for one disk revision."""

    revision: int
    content_segments: tuple[str, ...]
    index_segments: Mapping[str, tuple[str, ...]]
    specs: Mapping[str, IndexSpec]
    indexes: Mapping[str, Index]
    locations: Mapping[str, _Location]


class _DiskView(BackendView):
    """Read-only disk view retaining segment files for one revision."""

    def __init__(self, backend: "DiskBackend", state: _DiskState) -> None:
        """Capture a disk state and register it as an active view."""

        self._backend = backend
        self._state = state
        self._closed = False

    @property
    def revision(self) -> str:
        """Return the manifest revision as a token string."""

        return str(self._state.revision)

    @property
    def indexes(self) -> Mapping[str, IndexSpec]:
        """Return the index specifications published by this manifest."""

        return self._state.specs

    def scan(self) -> Iterator[DocumentData]:
        """Fetch every live document through the primary location index."""

        yield from self._backend._fetch_state(self._state, self._state.locations)

    def lookup(self, field: str, lookup: IndexLookup) -> set[str]:
        """Execute a lookup using the AVL cache loaded from index files."""

        try:
            index = self._state.indexes[field]
        except KeyError as error:
            raise BackendError(f"index does not exist: {field!r}") from error
        return _lookup_index(index, lookup)

    def fetch(self, document_ids: Iterable[str]) -> Iterator[DocumentData]:
        """Read only records selected through primary-index locations."""

        yield from self._backend._fetch_state(self._state, document_ids)

    def close(self) -> None:
        """Release this view and permit obsolete segment cleanup."""

        if not self._closed:
            self._closed = True
            self._backend._release_view(self._state.revision)


_OPEN_PATHS: set[Path] = set()
_OPEN_PATHS_LOCK = threading.Lock()


class DiskBackend(StorageBackend):
    """Directory backend with separate immutable content and index segments.

    ``manifest.json`` is the only commit marker. Segment files are written and
    synced first, then the manifest is atomically replaced. Consequently, files
    left by an interrupted commit are harmless and removed during cleanup.
    """

    FORMAT_VERSION = 2

    def __init__(self, path: str | os.PathLike[str]) -> None:
        """Prepare a directory backend without opening or locking it."""

        self.path = Path(path).expanduser().resolve()
        self.manifest_path = self.path / "manifest.json"
        self.lock_path = self.path / "database.lock"
        self._lock_file: object | None = None
        self._state: _DiskState | None = None
        self._opened = False
        self._closed = False
        self._active_views: dict[int, int] = {}
        self._lock = threading.RLock()

    def _check_open(self) -> _DiskState:
        """Return current disk state or reject a closed/unopened backend."""

        if not self._opened or self._closed or self._state is None:
            raise BackendError("disk backend is not open")
        return self._state

    def _acquire(self) -> None:
        """Acquire process-local and OS-level exclusive writer ownership."""

        with _OPEN_PATHS_LOCK:
            if self.path in _OPEN_PATHS:
                raise DatabaseLockedError(f"datastore is already open: {self.path}")
            _OPEN_PATHS.add(self.path)
        try:
            lock_file = open(self.lock_path, "a+b")
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_file.seek(0, os.SEEK_END)
                    if lock_file.tell() == 0:
                        lock_file.write(b"\0")
                        lock_file.flush()
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (OSError, BlockingIOError) as error:
                lock_file.close()
                raise DatabaseLockedError(
                    f"datastore is locked: {self.path}"
                ) from error
            self._lock_file = lock_file
        except BaseException:
            with _OPEN_PATHS_LOCK:
                _OPEN_PATHS.discard(self.path)
            raise

    @staticmethod
    def _spec_data(spec: IndexSpec) -> dict[str, object]:
        """Convert an index specification to manifest JSON data."""

        return {
            "field": spec.field,
            "unique": spec.unique,
            "sparse": spec.sparse,
            "expire_after_seconds": spec.expire_after_seconds,
        }

    @staticmethod
    def _parse_spec(value: object) -> IndexSpec:
        """Validate and decode one manifest index specification."""

        if not isinstance(value, Mapping) or not isinstance(value.get("field"), str):
            raise CorruptDataError("invalid index specification in manifest")
        if not isinstance(value.get("unique", False), bool) or not isinstance(
            value.get("sparse", False), bool
        ):
            raise CorruptDataError("invalid index flags in manifest")
        ttl = value.get("expire_after_seconds")
        if ttl is not None and not isinstance(ttl, (int, float)):
            raise CorruptDataError("invalid index TTL in manifest")
        try:
            return IndexSpec(
                str(value["field"]),
                bool(value.get("unique")),
                bool(value.get("sparse")),
                ttl,
            )
        except ValueError as error:
            raise CorruptDataError("invalid index specification in manifest") from error

    def _manifest_data(
        self,
        revision: int,
        content_segments: Iterable[str],
        specs: Mapping[str, IndexSpec],
        index_segments: Mapping[str, Iterable[str]],
    ) -> dict[str, object]:
        """Build the complete manifest object for a committed generation."""

        return {
            "format": self.FORMAT_VERSION,
            "revision": revision,
            "content_segments": list(content_segments),
            "indexes": {
                field: {
                    "spec": self._spec_data(spec),
                    "segments": list(index_segments.get(field, ())),
                }
                for field, spec in specs.items()
            },
        }

    def _atomic_manifest(self, data: Mapping[str, object]) -> None:
        """Fsync and atomically replace the manifest commit marker."""

        descriptor, temporary = tempfile.mkstemp(prefix=".manifest-", dir=self.path)
        published = False
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(_encode_json(data))
                stream.write(b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.manifest_path)
            published = True
            if os.name != "nt":
                try:
                    directory_fd = os.open(self.path, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                except OSError:
                    # The new manifest is already the visible commit marker.
                    # Reporting failure now would leave caller and disk state
                    # disagreeing, so directory syncing is best-effort on file
                    # systems that reject it.
                    pass
        except OSError as error:
            if not published:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass
                raise BackendError("failed to publish disk manifest") from error

    def _read_manifest(self) -> Mapping[str, object]:
        """Read and structurally validate the current manifest object."""

        try:
            value = _decode_json(self.manifest_path.read_bytes())
        except OSError as error:
            raise BackendError("failed to read disk manifest") from error
        if not isinstance(value, Mapping) or value.get("format") != self.FORMAT_VERSION:
            raise CorruptDataError("unsupported or corrupt disk manifest")
        if (
            not isinstance(value.get("revision"), int)
            or isinstance(value.get("revision"), bool)
            or not isinstance(value.get("content_segments"), list)
            or not isinstance(value.get("indexes"), Mapping)
        ):
            raise CorruptDataError("invalid disk manifest structure")
        return value

    def _segment_path(self, relative: str) -> Path:
        """Resolve a manifest path while preventing directory traversal."""

        candidate = (self.path / relative).resolve()
        if self.path not in candidate.parents:
            raise CorruptDataError("manifest segment escapes datastore directory")
        return candidate

    def _read_lines(self, relative: str) -> Iterator[Mapping[str, object]]:
        """Decode every complete JSON object in an immutable segment."""

        path = self._segment_path(relative)
        try:
            with open(path, "rb") as stream:
                for number, line in enumerate(stream, 1):
                    if not line.endswith(b"\n"):
                        raise CorruptDataError(
                            f"incomplete segment record in {relative}:{number}"
                        )
                    value = _decode_json(line[:-1])
                    if not isinstance(value, Mapping):
                        raise CorruptDataError(
                            f"non-object segment record in {relative}:{number}"
                        )
                    yield value
        except FileNotFoundError as error:
            raise CorruptDataError(
                f"manifest references missing segment: {relative}"
            ) from error
        except OSError as error:
            raise BackendError(f"failed to read segment: {relative}") from error

    def _write_lines(
        self, relative: str, values: Iterable[Mapping[str, object]]
    ) -> None:
        """Create and fsync one immutable JSON-lines segment."""

        path = self._segment_path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(path, "xb") as stream:
                for value in values:
                    stream.write(_encode_json(value))
                    stream.write(b"\n")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as error:
            raise BackendError(
                f"failed to write immutable segment: {relative}"
            ) from error

    @staticmethod
    def _index_directory(field: str) -> str:
        """Return a stable path-safe directory name for an index field."""

        digest = hashlib.sha256(field.encode("utf-8")).hexdigest()[:20]
        return f"indexes/{digest}"

    def _load_state(self, manifest: Mapping[str, object]) -> _DiskState:
        """Rebuild AVL caches and primary record locations from index files."""

        raw_indexes = manifest["indexes"]
        assert isinstance(raw_indexes, Mapping)
        specs: dict[str, IndexSpec] = {}
        index_segments: dict[str, tuple[str, ...]] = {}
        indexes: dict[str, Index] = {}
        locations: dict[str, _Location] = {}
        raw_content_segments = manifest["content_segments"]
        assert isinstance(raw_content_segments, list)
        if not all(isinstance(item, str) for item in raw_content_segments):
            raise CorruptDataError("content segment names must be strings")
        content_segments = tuple(raw_content_segments)
        content_segment_set = set(content_segments)
        for field, raw in raw_indexes.items():
            if (
                not isinstance(field, str)
                or not isinstance(raw, Mapping)
                or not isinstance(raw.get("segments"), list)
            ):
                raise CorruptDataError("invalid index entry in manifest")
            spec = self._parse_spec(raw.get("spec"))
            if field != spec.field:
                raise CorruptDataError(
                    "manifest index field does not match its specification"
                )
            if not all(isinstance(item, str) for item in raw["segments"]):
                raise CorruptDataError("index segment names must be strings")
            segments = tuple(raw["segments"])
            index = Index(spec)
            for segment in segments:
                for operation in self._read_lines(segment):
                    kind = operation.get("op")
                    document_id = operation.get("id")
                    if not isinstance(document_id, str) or "value" not in operation:
                        raise CorruptDataError("invalid persisted index operation")
                    if kind == "remove":
                        index.remove_value(operation["value"], document_id)
                        if field == "_id":
                            locations.pop(document_id, None)
                    elif kind == "add":
                        index.add_value(operation["value"], document_id)
                        if field == "_id":
                            raw_location = operation.get("location")
                            if not isinstance(raw_location, Mapping):
                                raise CorruptDataError(
                                    "primary index entry lacks a record location"
                                )
                            try:
                                location = _Location(
                                    str(raw_location["segment"]),
                                    int(raw_location["offset"]),
                                    int(raw_location["length"]),
                                )
                            except (KeyError, TypeError, ValueError) as error:
                                raise CorruptDataError(
                                    "invalid primary index location"
                                ) from error
                            if (
                                location.segment not in content_segment_set
                                or location.offset < 0
                                or location.length <= 0
                            ):
                                raise CorruptDataError(
                                    "primary index location is outside committed content"
                                )
                            locations[document_id] = location
                    else:
                        raise CorruptDataError("unknown persisted index operation")
            specs[field] = spec
            index_segments[field] = segments
            indexes[field] = index
        if specs.get("_id") != IndexSpec("_id", unique=True):
            raise CorruptDataError("manifest lacks the mandatory unique _id index")
        for segment in content_segments:
            if not self._segment_path(segment).is_file():
                raise CorruptDataError(
                    f"manifest references missing content segment: {segment}"
                )
        return _DiskState(
            int(manifest["revision"]),
            content_segments,
            MappingProxyType(index_segments),
            MappingProxyType(specs),
            MappingProxyType(indexes),
            MappingProxyType(locations),
        )

    def open(self) -> None:
        """Create/open the directory, lock it, and load only persisted indexes."""

        with self._lock:
            if self._closed:
                raise BackendError("disk backend is closed")
            if self._opened:
                return
            if self.path.exists() and not self.path.is_dir():
                raise BackendError(
                    "DiskBackend now requires a directory; legacy single-file stores are unsupported"
                )
            self.path.mkdir(parents=True, exist_ok=True)
            (self.path / "content").mkdir(exist_ok=True)
            (self.path / "indexes").mkdir(exist_ok=True)
            self._acquire()
            try:
                if not self.manifest_path.exists():
                    spec = IndexSpec("_id", unique=True)
                    self._atomic_manifest(
                        self._manifest_data(0, (), {"_id": spec}, {"_id": ()})
                    )
                self._state = self._load_state(self._read_manifest())
                self._opened = True
                self._cleanup_obsolete()
            except BaseException:
                self._release_lock()
                raise

    def view(self) -> BackendView:
        """Capture current manifest state and retain all referenced segments."""

        with self._lock:
            state = self._check_open()
            self._active_views[state.revision] = (
                self._active_views.get(state.revision, 0) + 1
            )
            return _DiskView(self, state)

    def _release_view(self, revision: int) -> None:
        """Release a revision reference and clean obsolete files when safe."""

        with self._lock:
            count = self._active_views.get(revision, 0)
            if count <= 1:
                self._active_views.pop(revision, None)
            else:
                self._active_views[revision] = count - 1
            if not self._active_views and self._opened and not self._closed:
                self._cleanup_obsolete()

    def _read_location(
        self, state: _DiskState, document_id: str
    ) -> DocumentData | None:
        """Read and validate one document from its primary-index byte location."""

        location = state.locations.get(document_id)
        if location is None:
            return None
        path = self._segment_path(location.segment)
        try:
            with open(path, "rb") as stream:
                stream.seek(location.offset)
                raw = stream.read(location.length)
        except OSError as error:
            raise BackendError(f"failed to fetch document {document_id!r}") from error
        value = _decode_json(raw)
        if (
            not isinstance(value, Mapping)
            or value.get("op") != "put"
            or not isinstance(value.get("document"), Mapping)
        ):
            raise CorruptDataError("primary index points to an invalid content record")
        document = _copy_document(value["document"])
        if document.get("_id") != document_id:
            raise CorruptDataError("primary index points to a different document")
        return document

    def _fetch_state(
        self, state: _DiskState, document_ids: Iterable[str]
    ) -> Iterator[DocumentData]:
        """Fetch selected IDs from a captured disk state."""

        for document_id in document_ids:
            document = self._read_location(state, document_id)
            if document is not None:
                yield document

    def _final_documents(
        self, state: _DiskState, changes: ChangeSet
    ) -> Iterator[DocumentData]:
        """Stream post-change documents when building a newly requested index."""

        replaced = set(changes.deletes) | {
            str(document["_id"]) for document in changes.puts
        }
        for document in self._fetch_state(state, state.locations):
            if str(document["_id"]) not in replaced:
                yield document
        for document in changes.puts:
            yield _copy_document(document)

    def _write_content_segment(
        self, revision: int, token: str, changes: ChangeSet
    ) -> tuple[str | None, dict[str, _Location]]:
        """Write document mutations and return locations for newly put records."""

        if not changes.puts and not changes.deletes:
            return None, {}
        relative = f"content/{revision:020d}-{token}.jsonl"
        path = self._segment_path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        locations: dict[str, _Location] = {}
        try:
            with open(path, "xb") as stream:
                for document_id in changes.deletes:
                    stream.write(
                        _encode_json({"op": "delete", "id": document_id}) + b"\n"
                    )
                for document in changes.puts:
                    payload = _encode_json({"op": "put", "document": document})
                    offset = stream.tell()
                    stream.write(payload + b"\n")
                    locations[str(document["_id"])] = _Location(
                        relative, offset, len(payload)
                    )
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as error:
            raise BackendError("failed to write content segment") from error
        return relative, locations

    def _write_snapshot_content(
        self, revision: int, token: str, documents: Iterable[Mapping[str, object]]
    ) -> tuple[str, dict[str, _Location]]:
        """Stream a consolidated content snapshot without materializing documents."""

        relative = f"content/{revision:020d}-{token}.jsonl"
        path = self._segment_path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        locations: dict[str, _Location] = {}
        try:
            with open(path, "xb") as stream:
                for document in documents:
                    payload = _encode_json({"op": "put", "document": document})
                    offset = stream.tell()
                    stream.write(payload + b"\n")
                    locations[str(document["_id"])] = _Location(
                        relative, offset, len(payload)
                    )
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as error:
            raise BackendError("failed to write compacted content segment") from error
        return relative, locations

    @staticmethod
    def _location_data(location: _Location) -> dict[str, object]:
        """Convert a primary-index location to persisted JSON data."""

        return {
            "segment": location.segment,
            "offset": location.offset,
            "length": location.length,
        }

    def _index_delta(
        self,
        field: str,
        index: Index,
        old_documents: Mapping[str, DocumentData],
        puts: tuple[Mapping[str, object], ...],
        new_locations: Mapping[str, _Location],
    ) -> list[dict[str, object]]:
        """Build remove/add operations for one existing index."""

        operations: list[dict[str, object]] = []
        for document_id, document in old_documents.items():
            for value in index.values(document):
                operations.append({"op": "remove", "value": value, "id": document_id})
        for document in puts:
            document_id = str(document["_id"])
            for value in index.values(document):
                operation: dict[str, object] = {
                    "op": "add",
                    "value": value,
                    "id": document_id,
                }
                if field == "_id":
                    operation["location"] = self._location_data(
                        new_locations[document_id]
                    )
                operations.append(operation)
        return operations

    def _full_index_records(
        self, field: str, index: Index, locations: Mapping[str, _Location]
    ) -> list[dict[str, object]]:
        """Export a complete index snapshot as add operations."""

        records: list[dict[str, object]] = []
        for value, document_ids in index.entries():
            for document_id in document_ids:
                operation: dict[str, object] = {
                    "op": "add",
                    "value": value,
                    "id": document_id,
                }
                if field == "_id":
                    operation["location"] = self._location_data(locations[document_id])
                records.append(operation)
        return records

    def commit(self, base_revision: str, changes: ChangeSet) -> str:
        """Stage content/index segments and atomically publish a new manifest."""

        with self._lock:
            state = self._check_open()
            if base_revision != str(state.revision):
                raise WriteConflictError("disk backend revision changed")
            if changes.empty:
                return base_revision
            if any(
                not isinstance(document.get("_id"), str) or not document.get("_id")
                for document in changes.puts
            ):
                raise BackendError("stored documents require a non-empty string _id")
            put_ids = [str(document["_id"]) for document in changes.puts]
            if len(set(put_ids)) != len(put_ids):
                duplicate = next(item for item in put_ids if put_ids.count(item) > 1)
                raise DuplicateKeyError("_id", duplicate)

            affected_ids = set(changes.deletes) | set(put_ids)
            old_documents = {
                document_id: document
                for document_id in affected_ids
                if (document := self._read_location(state, document_id)) is not None
            }
            specs = dict(state.specs)
            indexes = {field: index.clone() for field, index in state.indexes.items()}
            index_segments = {
                field: list(segments)
                for field, segments in state.index_segments.items()
            }
            for field in changes.drop_indexes:
                if field == "_id":
                    raise BackendError("the _id index cannot be removed")
                specs.pop(field, None)
                indexes.pop(field, None)
                index_segments.pop(field, None)
            created_fields: set[str] = set()
            for spec in changes.create_indexes:
                existing = specs.get(spec.field)
                if existing is not None and existing != spec:
                    raise BackendError(
                        f"index already exists with different options: {spec.field!r}"
                    )
                if existing is None:
                    specs[spec.field] = spec
                    created_fields.add(spec.field)

            for field, index in indexes.items():
                for document in old_documents.values():
                    index.remove(document)
                for document in changes.puts:
                    index.add(document)

            if created_fields:
                for field in created_fields:
                    indexes[field] = Index.build(
                        specs[field], self._final_documents(state, changes)
                    )
                    index_segments[field] = []

            revision = state.revision + 1
            token = uuid.uuid4().hex
            content_segment, new_locations = self._write_content_segment(
                revision, token, changes
            )
            locations = dict(state.locations)
            for document_id in affected_ids:
                locations.pop(document_id, None)
            locations.update(new_locations)
            content_segments = list(state.content_segments)
            if content_segment is not None:
                content_segments.append(content_segment)

            for field, index in indexes.items():
                if field in created_fields:
                    operations = self._full_index_records(field, index, locations)
                elif changes.puts or changes.deletes:
                    operations = self._index_delta(
                        field, index, old_documents, changes.puts, new_locations
                    )
                else:
                    operations = []
                if operations:
                    relative = (
                        f"{self._index_directory(field)}/{revision:020d}-{token}.jsonl"
                    )
                    self._write_lines(relative, operations)
                    index_segments.setdefault(field, []).append(relative)

            manifest = self._manifest_data(
                revision, content_segments, specs, index_segments
            )
            self._atomic_manifest(manifest)
            self._state = _DiskState(
                revision,
                tuple(content_segments),
                MappingProxyType(
                    {field: tuple(items) for field, items in index_segments.items()}
                ),
                MappingProxyType(specs),
                MappingProxyType(indexes),
                MappingProxyType(locations),
            )
            if not self._active_views:
                self._cleanup_obsolete()
            return str(revision)

    def compact(self) -> str:
        """Write consolidated content and index snapshots, then publish them."""

        with self._lock:
            state = self._check_open()
            revision = state.revision + 1
            token = uuid.uuid4().hex
            content_segment, locations = self._write_snapshot_content(
                revision, token, self._fetch_state(state, state.locations)
            )
            index_segments: dict[str, tuple[str, ...]] = {}
            for field, index in state.indexes.items():
                records = self._full_index_records(field, index, locations)
                relative = (
                    f"{self._index_directory(field)}/{revision:020d}-{token}.jsonl"
                )
                self._write_lines(relative, records)
                index_segments[field] = (relative,)
            content_segments = (content_segment,)
            self._atomic_manifest(
                self._manifest_data(
                    revision, content_segments, state.specs, index_segments
                )
            )
            self._state = _DiskState(
                revision,
                content_segments,
                MappingProxyType(index_segments),
                state.specs,
                state.indexes,
                MappingProxyType(locations),
            )
            if not self._active_views:
                self._cleanup_obsolete()
            return str(revision)

    def _cleanup_obsolete(self) -> None:
        """Delete unreferenced segment files when no active view needs them."""

        if self._active_views or self._state is None:
            return
        referenced = set(self._state.content_segments)
        for segments in self._state.index_segments.values():
            referenced.update(segments)
        for root in (self.path / "content", self.path / "indexes"):
            if not root.exists():
                continue
            for candidate in root.rglob("*.jsonl"):
                relative = candidate.relative_to(self.path).as_posix()
                if relative not in referenced:
                    try:
                        candidate.unlink()
                    except FileNotFoundError:
                        pass

    def _release_lock(self) -> None:
        """Release platform advisory locking and process-local registration."""

        if self._lock_file is not None:
            lock_file = self._lock_file
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_file.seek(0)  # type: ignore[attr-defined]
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
            finally:
                lock_file.close()  # type: ignore[attr-defined]
                self._lock_file = None
        with _OPEN_PATHS_LOCK:
            _OPEN_PATHS.discard(self.path)

    def close(self) -> None:
        """Release writer ownership; immutable views retain already-open state."""

        with self._lock:
            if self._closed:
                return
            self._release_lock()
            self._closed = True
