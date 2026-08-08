"""Implement and exercise a minimal lock-free avldb storage backend."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping, TypeAlias

from avldb import (
    BackendError,
    BackendView,
    ChangeSet,
    Collection,
    Document,
    DuplicateKeyError,
    IndexLookup,
    IndexSpec,
    StorageBackend,
    WriteConflictError,
)
from avldb.indexing import Index

DocumentData: TypeAlias = dict[str, object]
DocumentInput: TypeAlias = Mapping[str, object]
DocumentTable: TypeAlias = dict[str, DocumentData]
IndexSpecs: TypeAlias = Mapping[str, IndexSpec]
AVLIndexes: TypeAlias = Mapping[str, Index]


def _copy(document: DocumentInput) -> DocumentData:
    """Return a detached document so callers cannot mutate backend state."""

    return deepcopy(dict(document))


def _build_indexes(specs: IndexSpecs, documents: Iterable[DocumentInput]) -> AVLIndexes:
    """Load canonical documents into one immutable rs-avl index per field."""

    materialized = tuple(documents)
    return MappingProxyType(
        {field: Index.build(spec, materialized) for field, spec in specs.items()}
    )


def _lookup_avl(index: Index, lookup: IndexLookup) -> set[str]:
    """Execute one backend-neutral lookup against a loaded AVL index."""

    candidates: list[set[str]] = []
    if lookup.values is not None:
        candidates.append(index.matching_any(lookup.values))
    if lookup.lower is not None and lookup.upper is not None:
        candidates.append(
            index.between(
                lower=lookup.lower.value,
                upper=lookup.upper.value,
                include_lower=lookup.lower.inclusive,
                include_upper=lookup.upper.inclusive,
            )
        )
    elif lookup.lower is not None:
        candidates.append(
            index.between(
                lower=lookup.lower.value,
                include_lower=lookup.lower.inclusive,
            )
        )
    elif lookup.upper is not None:
        candidates.append(
            index.between(
                upper=lookup.upper.value,
                include_upper=lookup.upper.inclusive,
            )
        )

    if not candidates:
        raise BackendError("an index lookup requires exact values or a range")
    result: set[str] = candidates[0]
    for candidate in candidates[1:]:
        result &= candidate
    return result


@dataclass(frozen=True, slots=True)
class _State:
    """One immutable logical revision with loaded AVL indexes."""

    revision: int
    documents: tuple[DocumentData, ...]
    specs: IndexSpecs
    avl_indexes: AVLIndexes


class ListView(BackendView):
    """Stable read view over one copy-on-write list state."""

    def __init__(self, state: _State) -> None:
        """Capture a state that the backend will never mutate in place."""

        self._state: _State = state
        self._by_id: DocumentTable = {
            str(document["_id"]): document for document in state.documents
        }

    @property
    def revision(self) -> str:
        """Return the captured revision token."""

        return str(self._state.revision)

    @property
    def indexes(self) -> Mapping[str, IndexSpec]:
        """Return index definitions visible in this revision."""

        return self._state.specs

    def scan(self) -> Iterator[DocumentData]:
        """Stream detached copies of every live document."""

        for document in self._state.documents:
            yield _copy(document)

    def lookup(self, field: str, lookup: IndexLookup) -> set[str]:
        """Translate a planner lookup into exact and range AVL operations."""

        try:
            index = self._state.avl_indexes[field]
        except KeyError as error:
            raise BackendError(f"index does not exist: {field!r}") from error
        return _lookup_avl(index, lookup)

    def fetch(self, document_ids: Iterable[str]) -> Iterator[DocumentData]:
        """Yield detached documents in the requested ID order."""

        for document_id in document_ids:
            document = self._by_id.get(document_id)
            if document is not None:
                yield _copy(document)

    def close(self) -> None:
        """Close the view; this in-memory example owns no resources."""


class ListBackend(StorageBackend):
    """Minimal copy-on-write backend without locking or persistence."""

    def __init__(self) -> None:
        """Create an empty backend containing the mandatory ID index."""

        specs: dict[str, IndexSpec] = {"_id": IndexSpec("_id", unique=True)}
        frozen_specs: IndexSpecs = MappingProxyType(specs)
        self._state: _State = _State(
            0, (), frozen_specs, _build_indexes(frozen_specs, ())
        )
        self._open: bool = False

    def _check_open(self) -> None:
        """Reject operations when the backend is not open."""

        if not self._open:
            raise BackendError("list backend is not open")

    def open(self) -> None:
        """Open this resource-free backend."""

        self._open = True

    def view(self) -> BackendView:
        """Return a stable view of the current state object."""

        self._check_open()
        return ListView(self._state)

    def commit(self, base_revision: str, changes: ChangeSet) -> str:
        """Build and atomically publish a replacement state."""

        self._check_open()
        if base_revision != str(self._state.revision):
            raise WriteConflictError("list backend revision changed")
        if changes.empty:
            return base_revision

        documents: DocumentTable = {
            str(document["_id"]): _copy(document) for document in self._state.documents
        }
        for document_id in changes.deletes:
            documents.pop(document_id, None)
        put_ids: list[str] = []
        for document in changes.puts:
            document_id = document.get("_id")
            if not isinstance(document_id, str) or not document_id:
                raise BackendError("stored documents require a non-empty string _id")
            put_ids.append(document_id)
        if len(set(put_ids)) != len(put_ids):
            raise DuplicateKeyError("_id", "duplicate ID in one commit")
        for document in changes.puts:
            documents[str(document["_id"])] = _copy(document)

        specs: dict[str, IndexSpec] = dict(self._state.specs)
        for field in changes.drop_indexes:
            if field == "_id":
                raise BackendError("the _id index cannot be removed")
            specs.pop(field, None)
        for spec in changes.create_indexes:
            existing: IndexSpec | None = specs.get(spec.field)
            if existing is not None and existing != spec:
                raise BackendError(
                    f"index already exists with different options: {spec.field!r}"
                )
            specs[spec.field] = spec

        frozen_specs: IndexSpecs = MappingProxyType(specs)
        frozen_documents: tuple[DocumentData, ...] = tuple(documents.values())
        avl_indexes: AVLIndexes = _build_indexes(frozen_specs, frozen_documents)
        self._state = _State(
            self._state.revision + 1,
            frozen_documents,
            frozen_specs,
            avl_indexes,
        )
        return str(self._state.revision)

    def compact(self) -> str:
        """Publish an equivalent revision because lists need no compaction."""

        self._check_open()
        self._state = _State(
            self._state.revision + 1,
            tuple(_copy(document) for document in self._state.documents),
            self._state.specs,
            _build_indexes(self._state.specs, self._state.documents),
        )
        return str(self._state.revision)

    def close(self) -> None:
        """Close this resource-free backend."""

        self._open = False


class Task(Document):
    """Task stored by the custom-backend demonstration."""

    title: str
    priority: int
    tags: list[str]


def main() -> None:
    """Exercise equality, range planning, updates, and uniqueness."""

    with Collection(Task, backend=ListBackend()) as tasks:
        tasks.ensure_index("title", unique=True)
        tasks.ensure_index("priority")
        tasks.ensure_index("tags")
        tasks.insert_many(
            [
                Task(title="write docs", priority=2, tags=["docs"]),
                Task(title="ship package", priority=5, tags=["release", "python"]),
                Task(title="answer issues", priority=3, tags=["support", "python"]),
            ]
        )

        urgent = tasks.find({"priority": {"$gte": 3}}).sort({"priority": -1}).all()
        python_tasks = tasks.find({"tags": "python"}).all()
        tasks.update({"title": "write docs"}, {"$inc": {"priority": 2}})

        assert [task.title for task in urgent] == ["ship package", "answer issues"]
        assert {task.title for task in python_tasks} == {
            "ship package",
            "answer issues",
        }
        updated = tasks.find_one({"title": "write docs"})
        assert updated is not None
        assert updated.priority == 4
        print("Urgent:", [task.title for task in urgent])
        print("Python:", [task.title for task in python_tasks])


if __name__ == "__main__":
    main()
