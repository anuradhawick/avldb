# avldb

[![PyPI](https://img.shields.io/pypi/v/avldb.svg)](https://pypi.org/project/avldb/)
[![Python versions](https://img.shields.io/pypi/pyversions/avldb.svg)](https://pypi.org/project/avldb/)
[![CI](https://github.com/anuradhawick/avldb/actions/workflows/ci.yml/badge.svg)](https://github.com/anuradhawick/avldb/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/avldb.svg)](https://github.com/anuradhawick/avldb/blob/main/LICENSE)

`avldb` is a synchronous embedded document database for typed Pydantic models.
It provides NeDB-style CRUD and MongoDB-like queries while leaving record
placement, persistence, indexes, and caching to an extensible backend contract.

The built-in backends use the Rust-backed
[`rs-avl`](https://pypi.org/project/rs-avl/) ordered set for indexes, but custom
backends may use a different index engine.

## Installation

```console
pip install avldb
```

Python 3.10+ and CPython are supported.

## Quick start

```python
from pathlib import Path

from avldb import Collection, DiskBackend, Document


class User(Document):
    # This is application data. avldb's database ID remains user._id.
    id: int
    name: str
    age: int
    roles: list[str] = []


with Collection(User, backend=DiskBackend(Path("users.avldb"))) as users:
    users.ensure_index("id", unique=True)
    users.ensure_index("age")
    users.ensure_index("roles")

    users.insert(User(id=1001, name="Ada", age=36, roles=["admin", "author"]))
    users.insert(User(id=1002, name="Grace", age=29, roles=["author"]))

    adults = users.find({"age": {"$gte": 18}}).sort({"age": -1}).all()
    authors = users.find({"roles": "author"}).all()
```

Database identifiers are monotonic UUIDv7 strings. Access them as
`document._id`, query with `{"_id": value}`, and serialize with
`model_dump(by_alias=True)`. Models may independently define an `id` field.

Full reads return the collection's Pydantic model. Projected reads return plain
dictionaries because partial data may not satisfy the complete model schema.

## Queries and updates

Queries support:

- Equality and deep equality.
- Nested dot paths and arrays of subdocuments.
- `$lt`, `$lte`, `$gt`, `$gte`, `$in`, `$nin`, `$ne`, `$exists`, and `$regex`.
- `$size` and `$elemMatch` for arrays.
- `$or`, `$and`, `$not`, and callable `$where` expressions.
- Inclusion/exclusion projection, multi-field sorting, skip, and limit.

Updates may replace a document or use `$set`, `$unset`, `$inc`, `$min`, `$max`,
`$push`, `$pop`, `$addToSet`, and `$pull`, including `$each` and `$slice`.
Single-document updates are the default; `multi=True` updates or removes every
match. Upserts are supported.

```python
result = users.update(
    {"id": 1001},
    {"$inc": {"age": 1}, "$addToSet": {"roles": "maintainer"}},
    return_updated=True,
)
```

## How query execution works

`Collection` owns query semantics but never owns all documents or an index tree.
For an indexed range such as:

```python
users.find({"age": {"$gte": 18, "$lt": 65}})
```

the planner performs:

```text
BackendView.lookup("age", 18 <= value < 65)
    → candidate document IDs
BackendView.fetch(candidate IDs)
    → matching content records only
full NeDB matcher
    → final results
```

Multiple usable indexes are intersected. Fully indexed `$or` branches are
unioned. The full matcher always verifies candidates. If no safe index exists,
`BackendView.scan()` streams content instead of loading the collection into
memory.

Iterating an unsorted cursor also streams matched records and keeps its backend
view open for the iteration. Calling `.all()` intentionally materializes the
returned results, and sorting must materialize all matches before ordering them.

## Backends

The backend owns:

- Physical document placement and lookup.
- The mandatory unique `_id` index and all secondary indexes.
- Index persistence, caching, and implementation technology.
- Atomic content/index commits and revision conflict detection.
- Compaction and cleanup.

`Collection` uses only this small snapshot contract:

```python
class StorageBackend:
    def open(self) -> None: ...
    def view(self) -> BackendView: ...
    def commit(self, base_revision: str, changes: ChangeSet) -> str: ...
    def compact(self) -> str: ...
    def close(self) -> None: ...


class BackendView:
    revision: str
    indexes: Mapping[str, IndexSpec]

    def scan(self) -> Iterator[dict[str, object]]: ...
    def lookup(self, field: str, lookup: IndexLookup) -> set[str]: ...
    def fetch(self, document_ids: Iterable[str]) -> Iterator[dict[str, object]]: ...
```

Views represent immutable revisions. `commit` must apply document changes and
every affected index atomically or expose none of them. A stale revision raises
`WriteConflictError`. This makes queries, bulk insertion, unique constraints,
multi-updates, and index creation backend-independent.

### MemoryBackend

`MemoryBackend` is the smallest reference implementation:

- Documents live in a record array.
- The `_id` index resolves IDs to array slots.
- Secondary indexes use AVL trees.
- Commits build copy-on-write state and atomically swap one revision.
- Old backend views remain stable after later writes.

### DiskBackend

`DiskBackend` uses a directory, not a single database file:

```text
users.avldb/
├── manifest.json
├── database.lock
├── content/
│   └── <revision>.jsonl
└── indexes/
    ├── <_id-index>/
    │   └── <revision>.jsonl
    ├── <age-index>/
    │   └── <revision>.jsonl
    └── <roles-index>/
        └── <revision>.jsonl
```

Content and every index use separate immutable segments. A commit writes and
fsyncs all segments before atomically replacing `manifest.json`; the manifest is
the sole commit marker. Interrupted staging leaves unreferenced files that are
ignored and cleaned later.

The persisted `_id` index maps IDs to content segment byte offsets. Opening a
database reads index files but does not read every document. Indexed queries
fetch only candidate content records. Compaction writes consolidated content and
index snapshots, publishes a new manifest, then cleans segments no active view
needs.

Disk storage allows one writer backend for a datastore directory. Calls through
one collection are thread-safe and serialized.

### Source layout

The package is grouped by responsibility while keeping the root import API
small and stable:

```text
src/avldb/
├── core/        # Document models, matching, updates, cursors, and planning
├── indexing/    # The reusable rs-avl index adapter
├── storage/     # Backend contract, codecs, and memory/disk implementations
├── contracts.py # Shared public dataclasses and protocols
└── exceptions.py
```

Application code should generally import from `avldb`. Backend implementations
can import the extension contract from `avldb.storage`.

## Implementing another backend

Subclass `StorageBackend` and return a `BackendView` implementation. The shared
backend conformance tests illustrate the required behavior. Important rules:

1. A view must remain logically immutable until closed.
2. `lookup` returns document IDs, never physical locations.
3. `fetch` resolves IDs using backend-owned primary index state.
4. `scan` streams live documents and must not require preloading them.
5. `commit` validates its base revision and atomically updates documents,
   unique constraints, secondary indexes, and index metadata.
6. Failed commits must remain invisible.

An S3 implementation can map content/index segments to immutable objects and
the manifest to a small object updated conditionally with its ETag. It may cache
downloaded indexes as AVL trees, use another ordered structure, or query an
external index service without changing `Collection`.

No S3 dependency is bundled.

## Full example

The runnable example demonstrates typed models with a domain `id`, unique,
range, nested and multikey indexes, projections, updates, upsert, TTL cleanup,
reopen, and compaction:

```console
uv run python examples/basic.py
```

## Development and benchmarks

```console
uv sync
uv run pytest
uv run python benchmarks/indexed_queries.py
uv build --no-sources
```

The benchmark compares repeated indexed queries with streaming scans over the
same dataset. It contains no timing assertion because absolute performance is
environment-dependent.

Licensed under your choice of Apache-2.0 or GPL-3.0-only.
