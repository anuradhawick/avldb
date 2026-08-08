"""Typed collection, cursor, and NeDB-compatible backend query planner."""

from __future__ import annotations

import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Generic, Iterable, Iterator, Mapping

from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from .values import get_path, validate_storable
from ..storage import BackendView, MemoryBackend, StorageBackend
from .document import Document, new_document_id
from ..exceptions import (
    ClosedDatabaseError,
    DuplicateKeyError,
    QueryError,
    ValidationError,
)
from .query import (
    Projection,
    Query,
    SortSpec,
    apply_projection,
    apply_update,
    match_document,
    query_seed,
    sort_documents,
)
from ..contracts import (
    Bound,
    ChangeSet,
    DocumentT,
    IndexLookup,
    IndexSpec,
    UpdateResult,
)


class Cursor(Generic[DocumentT]):
    """A lazily executed, chainable collection query."""

    def __init__(self, collection: "Collection[DocumentT]", query: Query) -> None:
        """Capture a collection and copied query for lazy execution."""

        self._collection = collection
        self._query = deepcopy(dict(query))
        self._sort: dict[str, int] = {}
        self._skip = 0
        self._limit: int | None = None
        self._projection: dict[str, int | bool] = {}

    def sort(self, spec: SortSpec) -> "Cursor[DocumentT]":
        """Set an ordered mapping of sort paths to ascending/descending direction."""

        self._sort = dict(spec)
        return self

    def skip(self, count: int) -> "Cursor[DocumentT]":
        """Skip ``count`` matching documents after sorting."""

        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise QueryError("skip must be a non-negative integer")
        self._skip = count
        return self

    def limit(self, count: int | None) -> "Cursor[DocumentT]":
        """Limit results to ``count`` documents, or clear the limit with ``None``."""

        if count is not None and (
            not isinstance(count, int) or isinstance(count, bool) or count < 0
        ):
            raise QueryError("limit must be a non-negative integer or None")
        self._limit = count
        return self

    def projection(self, spec: Projection) -> "Cursor[DocumentT]":
        """Set the projection applied after matching, sorting, and pagination."""

        self._projection = dict(spec)
        return self

    def all(self) -> list[DocumentT] | list[dict[str, object]]:
        """Execute and materialize all model or projected mapping results."""

        return list(self._collection._iterate_cursor(self))

    def first(self) -> DocumentT | dict[str, object] | None:
        """Execute with a one-document limit and return the first result if any."""

        results = self.limit(1).all()
        return results[0] if results else None

    def __iter__(self) -> Iterator[DocumentT | dict[str, object]]:
        """Stream results when no sort requires materializing the matches."""

        return self._collection._iterate_cursor(self)


class Collection(Generic[DocumentT]):
    """Pydantic validation and NeDB query planning over a storage backend.

    The collection owns no document cache or index tree. It translates queries
    into backend index lookups, falls back to streaming scans, applies the full
    matcher, and submits atomic change sets for backend-controlled persistence.
    """

    def __init__(
        self, model: type[DocumentT], *, backend: StorageBackend | None = None
    ) -> None:
        """Bind a document model and open the supplied or default memory backend."""

        if not isinstance(model, type) or not issubclass(model, Document):
            raise TypeError("collection model must subclass avldb.Document")
        self.model = model
        self.backend = backend if backend is not None else MemoryBackend()
        self._lock = threading.RLock()
        self._closed = False
        try:
            self.backend.open()
        except BaseException:
            self.backend.close()
            self._closed = True
            raise

    def _check_open(self) -> None:
        """Raise when an operation is attempted after collection closure."""

        if self._closed:
            raise ClosedDatabaseError("collection is closed")

    def _canonical(self, value: DocumentT | Mapping[str, object]) -> dict[str, object]:
        """Validate and deep-copy one model or mapping into storage representation."""

        try:
            source: object = (
                value.model_dump(mode="python", by_alias=True)
                if isinstance(value, BaseModel)
                else dict(value)
            )
            model = self.model.model_validate(source)
        except (PydanticValidationError, TypeError, ValueError) as error:
            raise ValidationError(
                f"document does not satisfy {self.model.__name__}"
            ) from error
        document = model.model_dump(mode="python", by_alias=True)
        validate_storable(document)
        document_id = document.get("_id")
        if not isinstance(document_id, str) or not document_id:
            raise ValidationError("document id must be a non-empty string")
        return deepcopy(document)

    def _model(self, document: Mapping[str, object]) -> DocumentT:
        """Validate a copied backend document into the collection's model."""

        try:
            return self.model.model_validate(deepcopy(dict(document)))
        except PydanticValidationError as error:
            raise ValidationError(
                "backend document does not satisfy the collection model"
            ) from error

    def insert(self, document: DocumentT | Mapping[str, object]) -> DocumentT:
        """Validate and atomically insert one document."""

        return self.insert_many([document])[0]

    def insert_many(
        self, documents: Iterable[DocumentT | Mapping[str, object]]
    ) -> list[DocumentT]:
        """Validate and atomically insert all supplied documents."""

        with self._lock:
            self._check_open()
            prepared = [self._canonical(document) for document in documents]
            if not prepared:
                return []
            ids = [str(document["_id"]) for document in prepared]
            if len(set(ids)) != len(ids):
                duplicate = next(item for item in ids if ids.count(item) > 1)
                raise DuplicateKeyError("_id", duplicate)
            with self.backend.view() as view:
                existing = view.lookup("_id", IndexLookup.any(tuple(ids)))
                revision = view.revision
            if existing:
                duplicate = next(item for item in ids if item in existing)
                raise DuplicateKeyError("_id", duplicate)
            self.backend.commit(revision, ChangeSet(puts=tuple(prepared)))
            return [self._model(document) for document in prepared]

    def ensure_index(
        self,
        field: str | IndexSpec,
        *,
        unique: bool = False,
        sparse: bool = False,
        expire_after_seconds: float | None = None,
    ) -> None:
        """Ask the backend to atomically build and persist a secondary index."""

        spec = (
            field
            if isinstance(field, IndexSpec)
            else IndexSpec(field, unique, sparse, expire_after_seconds)
        )
        with self._lock:
            self._check_open()
            with self.backend.view() as view:
                existing = view.indexes.get(spec.field)
                revision = view.revision
            if existing is not None:
                if existing != spec:
                    raise ValidationError(
                        f"index {spec.field!r} already exists with different options"
                    )
                return
            self.backend.commit(revision, ChangeSet(create_indexes=(spec,)))

    def remove_index(self, field: str) -> None:
        """Ask the backend to remove a secondary index and its persisted state."""

        with self._lock:
            self._check_open()
            if field == "_id":
                raise ValidationError("the _id index cannot be removed")
            with self.backend.view() as view:
                exists = field in view.indexes
                revision = view.revision
            if exists:
                self.backend.commit(revision, ChangeSet(drop_indexes=(field,)))

    def _field_candidates(
        self, view: BackendView, field: str, condition: object
    ) -> set[str] | None:
        """Translate one indexed query condition into a backend lookup request."""

        if field not in view.indexes:
            return None
        if isinstance(condition, Mapping):
            values: tuple[object, ...] | None = None
            if "$in" in condition:
                raw_values = condition["$in"]
                if not isinstance(raw_values, (list, tuple)):
                    raise QueryError("$in requires an array")
                if any(
                    isinstance(value, (Mapping, list, tuple))
                    or hasattr(value, "search")
                    for value in raw_values
                ):
                    return None
                values = tuple(raw_values)
            bounds = {
                name: condition[name]
                for name in ("$gt", "$gte", "$lt", "$lte")
                if name in condition
            }
            lower = None
            upper = None
            if bounds:
                if "$gt" in bounds:
                    lower = Bound(bounds["$gt"], False)
                elif "$gte" in bounds:
                    lower = Bound(bounds["$gte"], True)
                if "$lt" in bounds:
                    upper = Bound(bounds["$lt"], False)
                elif "$lte" in bounds:
                    upper = Bound(bounds["$lte"], True)
            if values is None and lower is None and upper is None:
                return None
            return view.lookup(
                field, IndexLookup(values=values, lower=lower, upper=upper)
            )
        if isinstance(condition, (list, tuple)) or hasattr(condition, "search"):
            return None
        return view.lookup(field, IndexLookup.equal(condition))

    def _candidate_ids(self, view: BackendView, query: Query) -> set[str] | None:
        """Intersect safe field/AND candidates and union fully indexed OR branches."""

        candidate_sets: list[set[str]] = []
        for field, condition in query.items():
            if field == "$and":
                if not isinstance(condition, (list, tuple)):
                    raise QueryError("$and requires an array")
                for branch in condition:
                    if not isinstance(branch, Mapping):
                        raise QueryError("$and branches must be mappings")
                    candidates = self._candidate_ids(view, branch)
                    if candidates is not None:
                        candidate_sets.append(candidates)
            elif field == "$or":
                if not isinstance(condition, (list, tuple)):
                    raise QueryError("$or requires an array")
                branches: list[set[str]] = []
                for branch in condition:
                    if not isinstance(branch, Mapping):
                        raise QueryError("$or branches must be mappings")
                    candidates = self._candidate_ids(view, branch)
                    if candidates is None:
                        branches = []
                        break
                    branches.append(candidates)
                if branches:
                    candidate_sets.append(set().union(*branches))
            elif not field.startswith("$"):
                candidates = self._field_candidates(view, field, condition)
                if candidates is not None:
                    candidate_sets.append(candidates)
        if not candidate_sets:
            return None
        result = candidate_sets[0].copy()
        for candidates in candidate_sets[1:]:
            result &= candidates
        return result

    def _matching_documents(
        self, view: BackendView, query: Query
    ) -> Iterator[dict[str, object]]:
        """Stream full-matcher results from indexed fetches or a backend scan."""

        candidates = self._candidate_ids(view, query)
        source = view.scan() if candidates is None else view.fetch(candidates)
        for document in source:
            if match_document(document, query):
                yield document

    @staticmethod
    def _is_expired(
        document: Mapping[str, object], spec: IndexSpec, now: datetime
    ) -> bool:
        """Verify one TTL field against the current time."""

        value = get_path(document, spec.field)
        if not isinstance(value, datetime) or spec.expire_after_seconds is None:
            return False
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return now.timestamp() > value.timestamp() + spec.expire_after_seconds

    def _expire_stale_locked(self) -> int:
        """Use TTL range indexes to identify and atomically remove expired records."""

        now = datetime.now(timezone.utc)
        with self.backend.view() as view:
            ttl_specs = [
                spec
                for spec in view.indexes.values()
                if spec.expire_after_seconds is not None
            ]
            if not ttl_specs:
                return 0
            candidate_ids: set[str] = set()
            for spec in ttl_specs:
                assert spec.expire_after_seconds is not None
                cutoff = now - timedelta(seconds=spec.expire_after_seconds)
                candidate_ids.update(
                    view.lookup(
                        spec.field, IndexLookup.range(upper=Bound(cutoff, False))
                    )
                )
            expired = {
                str(document["_id"])
                for document in view.fetch(candidate_ids)
                if any(self._is_expired(document, spec, now) for spec in ttl_specs)
            }
            revision = view.revision
        if expired:
            self.backend.commit(revision, ChangeSet(deletes=tuple(sorted(expired))))
        return len(expired)

    def cleanup_expired(self) -> int:
        """Remove expired TTL documents immediately and return their count."""

        with self._lock:
            self._check_open()
            return self._expire_stale_locked()

    def find(
        self, query: Query | None = None, projection: Projection | None = None
    ) -> Cursor[DocumentT]:
        """Create a lazy cursor for a query and optional projection."""

        self._check_open()
        cursor = Cursor(self, query or {})
        if projection is not None:
            cursor.projection(projection)
        return cursor

    def find_one(
        self, query: Query | None = None, projection: Projection | None = None
    ) -> DocumentT | dict[str, object] | None:
        """Return the first matching model/projected mapping, or ``None``."""

        return self.find(query, projection).first()

    def count(self, query: Query | None = None) -> int:
        """Count documents using the same index-or-stream query plan as ``find``."""

        with self._lock:
            self._check_open()
            self._expire_stale_locked()
            with self.backend.view() as view:
                return sum(1 for _ in self._matching_documents(view, query or {}))

    def _iterate_cursor(
        self, cursor: Cursor[DocumentT]
    ) -> Iterator[DocumentT | dict[str, object]]:
        """Stream cursor results, materializing only when explicit sorting requires it."""

        with self._lock:
            self._check_open()
            self._expire_stale_locked()
            with self.backend.view() as view:
                documents: Iterable[dict[str, object]]
                if cursor._sort:
                    documents = sort_documents(
                        [
                            deepcopy(document)
                            for document in self._matching_documents(
                                view, cursor._query
                            )
                        ],
                        cursor._sort,
                    )
                else:
                    documents = self._matching_documents(view, cursor._query)
                skipped = 0
                emitted = 0
                for document in documents:
                    if skipped < cursor._skip:
                        skipped += 1
                        continue
                    if cursor._limit is not None and emitted >= cursor._limit:
                        break
                    emitted += 1
                    if cursor._projection:
                        yield apply_projection(document, cursor._projection)
                    else:
                        yield self._model(document)

    def update(
        self,
        query: Query,
        update: Mapping[str, object],
        *,
        multi: bool = False,
        upsert: bool = False,
        return_updated: bool = False,
    ) -> UpdateResult[DocumentT]:
        """Plan, validate, and atomically submit replacement/modifier updates."""

        with self._lock:
            self._check_open()
            self._expire_stale_locked()
            with self.backend.view() as view:
                matched = list(self._matching_documents(view, query))
                revision = view.revision
            if not multi:
                matched = matched[:1]
            if not matched:
                if not upsert:
                    return UpdateResult(0)
                seed = query_seed(query)
                seed.setdefault("_id", new_document_id())
                candidate = (
                    apply_update(seed, update)
                    if any(str(key).startswith("$") for key in update)
                    else dict(update)
                )
                inserted = self._canonical(candidate)
                self.backend.commit(revision, ChangeSet(puts=(inserted,)))
                return UpdateResult(1, (self._model(inserted),), True)

            changed: list[dict[str, object]] = []
            for original in matched:
                modified = apply_update(original, update)
                canonical = self._canonical(modified)
                if canonical["_id"] != original["_id"]:
                    raise QueryError("cannot change a document id")
                changed.append(canonical)
            self.backend.commit(revision, ChangeSet(puts=tuple(changed)))
            returned = (
                tuple(self._model(document) for document in changed)
                if return_updated
                else ()
            )
            return UpdateResult(len(changed), returned, False)

    def remove(self, query: Query, *, multi: bool = False) -> int:
        """Find and atomically delete the first or all matching documents."""

        with self._lock:
            self._check_open()
            with self.backend.view() as view:
                matched = [
                    str(document["_id"])
                    for document in self._matching_documents(view, query)
                ]
                revision = view.revision
            if not multi:
                matched = matched[:1]
            if matched:
                self.backend.commit(revision, ChangeSet(deletes=tuple(matched)))
            return len(matched)

    def compact(self) -> None:
        """Delegate representation-specific compaction to the backend."""

        with self._lock:
            self._check_open()
            self.backend.compact()

    def close(self) -> None:
        """Idempotently close the backend and collection."""

        with self._lock:
            if not self._closed:
                self.backend.close()
                self._closed = True

    def __enter__(self) -> "Collection[DocumentT]":
        """Return this open collection for context-manager use."""

        self._check_open()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close on context exit without suppressing exceptions."""

        self.close()
