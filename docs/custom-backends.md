# Writing a custom backend

An `avldb` backend owns document placement, index execution, snapshots,
revisions, atomic commits, and resource cleanup. The collection layer owns
Pydantic validation, NeDB query matching, query planning, updates, and CRUD.

The complete runnable example is
[`examples/custom_backend.py`](../examples/custom_backend.py). It implements a
copy-on-write list backend and uses `avldb.indexing.Index`, the reusable adapter
over `rs-avl`. It deliberately has no locking or persistence, so it is an
educational example rather than a production backend.

Run it from the repository root:

```console
uv run python examples/custom_backend.py
```

## Mental model

There are two paths through a backend. Reads happen against a stable
`BackendView`; writes replace one backend revision with another:

```text
indexed read
Collection → view.indexes → view.lookup(...) → document IDs
           → view.fetch(IDs) → full query matcher → Pydantic models

unindexed read
Collection → view.scan() → full query matcher → Pydantic models

write
Collection → validated ChangeSet + view.revision
           → backend.commit(revision, changes) → new revision
```

The planner never asks where a document lives and never touches an AVL tree.
It deals only in index descriptions, document IDs, and canonical document
mappings. That boundary lets a backend store documents in a list, byte ranges,
database rows, or object-storage segments.

## Type vocabulary

Giving storage concepts names makes the implementation much easier to follow.
The runnable example uses these Python 3.10-compatible aliases:

```python
from collections.abc import Mapping
from typing import TypeAlias

from avldb import IndexSpec
from avldb.indexing import Index

DocumentData: TypeAlias = dict[str, object]
DocumentInput: TypeAlias = Mapping[str, object]
DocumentTable: TypeAlias = dict[str, DocumentData]
IndexSpecs: TypeAlias = Mapping[str, IndexSpec]
AVLIndexes: TypeAlias = Mapping[str, Index]
```

- `DocumentInput` is read-only input accepted by helper functions.
- `DocumentData` is a mutable, detached canonical document containing `_id`.
- `DocumentTable` maps database IDs to current documents.
- `IndexSpecs` is public metadata the query planner can inspect.
- `AVLIndexes` is private backend state that actually executes lookups.

`IndexSpec` and `Index` are deliberately separate. The view exposes specs to
`Collection`, while the backend keeps its physical AVL objects private.

## `IndexSpec` and `Index`

An `IndexSpec` describes **what index exists**. It is small, immutable metadata
that can be persisted in a manifest or database catalog:

```python
from avldb import IndexSpec

priority_spec = IndexSpec("priority")
email_spec = IndexSpec("email", unique=True)
nickname_spec = IndexSpec("nickname", sparse=True)
expiry_spec = IndexSpec("created_at", expire_after_seconds=3600)
```

Its fields mean:

| Field | Meaning |
| --- | --- |
| `field` | Dotted document path to index, such as `profile.email`. |
| `unique` | Reject two documents that produce the same indexed value. |
| `sparse` | Do not add documents where the indexed path is missing. |
| `expire_after_seconds` | Treat a datetime field as a lazy TTL expiry source. |

`BackendView.indexes` returns a mapping of field names to these specifications:

```python
specs: IndexSpecs = {
    "_id": IndexSpec("_id", unique=True),
    "priority": IndexSpec("priority"),
}
```

The collection reads this mapping when planning a query. If `priority` is
present, it may call `view.lookup("priority", lookup)`. If it is absent, the
collection streams `view.scan()` instead. The mandatory `_id` specification is
always unique and cannot be removed.

An `Index` is **the loaded data structure that implements one specification**.
It wraps an `rs_avl.AVLTree` and maps normalized field values to document-ID
buckets:

```text
AVL key 1   → {document-a}
AVL key 3   → {document-b, document-c}
AVL key 10  → {document-d}
```

The bucket is necessary because `rs-avl` is an ordered set: a non-unique index
can have many documents with the same value, but the AVL tree still stores only
one node for that value. A unique specification limits each bucket to one
document ID.

Create an empty physical index from its specification, or build and validate it
from documents:

```python
from avldb.indexing import Index

empty_priority_index: Index = Index(priority_spec)
loaded_priority_index: Index = Index.build(priority_spec, documents)
```

`Index.build()` extracts the configured dotted path from every document. Arrays
produce multikey entries, repeated values within one document are deduplicated,
sparse indexes skip missing paths, and unique conflicts raise
`DuplicateKeyError`. Values are converted to explicit type-ranked AVL keys, so
Python equality quirks such as `True == 1` do not merge database values.

The main physical-index operations are:

| Operation | Purpose |
| --- | --- |
| `matching(value)` | Return IDs in one exact-value bucket. |
| `matching_any(values)` | Union IDs from several exact-value buckets. |
| `between(...)` | Range-scan ordered AVL buckets. |
| `add(document)` / `remove(document)` | Maintain an index from documents. |
| `add_value(value, id)` | Restore one persisted value-to-ID entry. |
| `entries()` | Export buckets for backend-specific persistence. |
| `clone()` | Create an independent mutable copy of an index. |

In this example, `_State.specs` contains planner-visible descriptions and
`_State.avl_indexes` contains the corresponding loaded trees. Both mappings use
the same field names, but they serve different consumers:

```text
Collection/query planner → state.specs["priority"]
Backend view lookup      → state.avl_indexes["priority"]
```

Keeping these separate also lets another backend honor `IndexSpec` using a
B-tree, a persisted index file, or a remote index service instead of the
provided AVL implementation.

## The two-part contract

A backend exposes five lifecycle and mutation methods. Every method has a
specific ownership rule:

| Method | Responsibility |
| --- | --- |
| `open()` | Acquire resources and prepare the first readable state. |
| `view()` | Capture one stable revision for a read operation. |
| `commit()` | Atomically turn a revision and `ChangeSet` into a new revision. |
| `compact()` | Rewrite backend representation without changing logical data. |
| `close()` | Release files, clients, writer ownership, or other resources. |

```python
from avldb import BackendView, ChangeSet, StorageBackend


class MyBackend(StorageBackend):
    def open(self) -> None: ...
    def view(self) -> BackendView: ...
    def commit(self, base_revision: str, changes: ChangeSet) -> str: ...
    def compact(self) -> str: ...
    def close(self) -> None: ...
```

Each call to `view()` returns a stable logical snapshot. The return types make
the separation between metadata, candidate IDs, and document content explicit:

```python
from collections.abc import Iterable, Iterator, Mapping
from typing import TypeAlias

from avldb import BackendView, IndexLookup, IndexSpec

DocumentData: TypeAlias = dict[str, object]


class MyView(BackendView):
    @property
    def revision(self) -> str: ...

    @property
    def indexes(self) -> Mapping[str, IndexSpec]: ...

    def scan(self) -> Iterator[DocumentData]: ...
    def lookup(self, field: str, lookup: IndexLookup) -> set[str]: ...
    def fetch(self, document_ids: Iterable[str]) -> Iterator[DocumentData]: ...
    def close(self) -> None: ...
```

`Collection` calls `lookup()` for safe indexed equality, `$in`, and range
conditions. It then calls `fetch()` for those candidate IDs and applies the
complete query matcher. When no suitable index exists, it calls `scan()`.

The `_id` index must always be present and unique. Secondary index definitions
appear only after their creation commit succeeds.

## The example state

The list backend keeps one immutable state object per revision:

```python
@dataclass(frozen=True, slots=True)
class _State:
    revision: int
    documents: tuple[DocumentData, ...]
    specs: IndexSpecs
    avl_indexes: AVLIndexes
```

`ListView` captures one `_State`. A commit constructs new documents and index
metadata, builds replacement AVL trees, and swaps `self._state` only after
everything has succeeded. Existing views therefore remain stable without
copying the whole database for every read.

The dataclass being frozen does not magically make its dictionaries or AVL
trees immutable. The example achieves logical immutability by convention: once
a `_State` is published, neither its documents nor its indexes are mutated.
Every commit builds replacements. `scan()` and `fetch()` also deep-copy returned
documents so callers cannot modify captured state.

## Loading documents into rs-avl

`rs-avl` is an ordered set, so duplicate raw values would collapse. The
`avldb.indexing.Index` adapter stores one AVL bucket per normalized value and a
set of document IDs inside each bucket. It also handles unique, sparse, nested,
and multikey indexes, and keeps values such as `True` and `1` distinct.

Build every declared index from a document iterable when opening storage or
publishing a new immutable state:

```python
from collections.abc import Iterable
from types import MappingProxyType

from avldb import IndexSpec
from avldb.indexing import Index


def build_indexes(
    specs: IndexSpecs,
    documents: Iterable[DocumentInput],
) -> AVLIndexes:
    materialized: tuple[DocumentInput, ...] = tuple(documents)
    return MappingProxyType(
        {
            field: Index.build(spec, materialized)
            for field, spec in specs.items()
        }
    )


specs: dict[str, IndexSpec] = {
    "_id": IndexSpec("_id", unique=True),
    "priority": IndexSpec("priority"),
    "tags": IndexSpec("tags"),  # Arrays become multikey AVL entries.
}
avl_indexes: AVLIndexes = build_indexes(specs, documents)
```

`Index.build()` raises `DuplicateKeyError` before returning if a unique index
cannot be built. Construct all replacement indexes before publishing the new
state so a failure leaves the previous revision untouched.

If a backend persists index buckets separately from documents, it can load
them without rescanning content:

```python
from collections.abc import Iterable
from typing import TypeAlias

PersistedBucket: TypeAlias = tuple[object, tuple[str, ...]]


def load_index(
    spec: IndexSpec,
    persisted_buckets: Iterable[PersistedBucket],
) -> Index:
    index: Index = Index(spec)
    for value, document_ids in persisted_buckets:
        for document_id in document_ids:
            index.add_value(value, document_id)
    return index
```

`index.entries()` returns the corresponding `(value, document_ids)` records for
persistence. A backend chooses the encoding, files, objects, and caching policy.

## Executing planner lookups

The view translates backend-neutral `IndexLookup` values into AVL operations.
Exact values use `matching_any()` and ordered bounds use `between()`:

```python
def lookup_avl(index: Index, lookup: IndexLookup) -> set[str]:
    candidates: list[set[str]] = []

    if lookup.values is not None:
        candidates.append(index.matching_any(lookup.values))
    if lookup.lower is not None and lookup.upper is not None:
        candidates.append(index.between(
            lower=lookup.lower.value,
            upper=lookup.upper.value,
            include_lower=lookup.lower.inclusive,
            include_upper=lookup.upper.inclusive,
        ))
    elif lookup.lower is not None:
        candidates.append(index.between(
            lower=lookup.lower.value,
            include_lower=lookup.lower.inclusive,
        ))
    elif lookup.upper is not None:
        candidates.append(index.between(
            upper=lookup.upper.value,
            include_upper=lookup.upper.inclusive,
        ))

    result: set[str] = candidates[0]
    for candidate in candidates[1:]:
        result &= candidate
    return result
```

The `IndexLookup` constructor guarantees at least one exact or range component,
so `candidates` is non-empty. When exact values and bounds are both present,
intersect their ID sets. The collection applies its full matcher afterward, so
an index may safely return false-positive candidates but must never omit a
possible match.

The public view method is then only field resolution plus delegation:

```python
def lookup(self, field: str, lookup: IndexLookup) -> set[str]:
    try:
        index: Index = self._state.avl_indexes[field]
    except KeyError as error:
        raise BackendError(f"index does not exist: {field!r}") from error
    return lookup_avl(index, lookup)
```

`fetch()` is different from `lookup()`: it receives IDs already selected by
the planner and resolves them through the backend's primary location mapping.
Its output order should follow the requested ID iterable when practical.

## Commit rules

`ChangeSet` contains complete canonical documents and metadata operations:

- `puts`: documents to insert or replace, each with a string `_id`.
- `deletes`: document IDs to remove.
- `create_indexes`: new `IndexSpec` definitions.
- `drop_indexes`: index fields to remove.

A backend commit must:

1. Reject a stale `base_revision` with `WriteConflictError`.
2. Preserve the mandatory unique `_id` index.
3. Apply document and index changes as one atomic operation.
4. Enforce unique index constraints and raise `DuplicateKeyError` on failure.
5. Publish a new revision only after the complete operation succeeds.
6. Leave the previous state visible if any validation or storage step fails.

The example implements those rules in four phases:

```text
1. Check base_revision
2. Copy current documents/specs and apply the ChangeSet to those copies
3. Build every AVL index (this validates unique constraints)
4. Assign one new _State
```

The assignment in phase 4 is the only publication point. If document handling
or `Index.build()` raises during phases 2 or 3, `self._state` still references
the complete previous revision. A filesystem or S3 backend needs an equivalent
single publication point, normally an atomically replaced or conditionally
updated manifest.

Every returned document must be detached from backend state. Otherwise a user
could mutate stored data without going through validation or `commit()`.

`WriteConflictError` is important even in this lock-free example. A view might
capture revision `4`; if another commit publishes revision `5`, committing work
prepared from revision `4` must fail instead of overwriting newer data.

## Index lookup semantics

`IndexLookup` represents exact values, an ordered range, or both:

```python
lookup.values       # equality or $in values
lookup.lower.value
lookup.lower.inclusive
lookup.upper.value
lookup.upper.inclusive
```

When both membership and range constraints are present, a returned ID must
satisfy both. Multikey fields should return a document ID when at least one
array value satisfies the complete lookup. Deduplicate repeated values from the
same document before applying unique constraints.

The provided AVL adapter supplies explicit type-ranked keys so values such as
`True` and `1` do not collapse into one index entry. Sparse indexes omit missing
values. TTL index definitions are exposed through
`IndexSpec.expire_after_seconds`; lazy expiry is initiated by `Collection`,
while the backend performs the resulting deletion commit.

## Moving toward production

The example omits several things a durable or concurrent backend needs:

- Serialize operations within a process and define writer ownership.
- Persist index definitions and rebuild indexes with `Index.build()`, or restore
  saved buckets with `Index.add_value()`, during `open()`.
- Make content and index publication atomic.
- Keep resources used by old views valid until those views close.
- Define crash recovery and corruption behavior.
- Exercise the shared behaviors demonstrated in `tests/test_persistence.py`.

The `Collection` instance serializes its own operations, but this example
backend does not coordinate direct backend calls or multiple collections. That
is what “without locking” means here. Add backend-level synchronization before
using a similar design across threads or collection instances.

For object storage, immutable content/index objects plus a conditionally updated
manifest are a natural fit. The manifest revision or ETag becomes the revision
token, and `fetch()` resolves candidate IDs to content object locations.
