from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pytest
from pydantic import ConfigDict, Field

from avldb import (
    BackendError,
    Bound,
    ChangeSet,
    Collection,
    CorruptDataError,
    DatabaseLockedError,
    DiskBackend,
    Document,
    DuplicateKeyError,
    IndexLookup,
    IndexSpec,
    MemoryBackend,
    StorageBackend,
    WriteConflictError,
)


class Note(Document):
    title: str
    priority: int = 0


class FlexibleRecord(Document):
    model_config = ConfigDict(populate_by_name=True, extra="allow")
    name: str
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, int] = Field(default_factory=dict)


def raw(document_id: str, title: str, priority: int) -> dict[str, object]:
    return {"_id": document_id, "title": title, "priority": priority}


@pytest.fixture(params=["memory", "disk"])
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[StorageBackend]:
    value: StorageBackend
    if request.param == "memory":
        value = MemoryBackend()
    else:
        value = DiskBackend(tmp_path / "contract.avldb")
    value.open()
    try:
        yield value
    finally:
        value.close()


def test_backend_conformance_lookup_fetch_scan_conflict_and_compaction(
    backend: StorageBackend,
) -> None:
    with backend.view() as initial:
        assert initial.revision == "0"
        assert initial.indexes == {"_id": IndexSpec("_id", unique=True)}
        assert list(initial.scan()) == []
        revision = initial.revision

    revision = backend.commit(
        revision,
        ChangeSet(
            puts=(raw("a", "alpha", 1), raw("b", "beta", 5), raw("c", "gamma", 9))
        ),
    )
    revision = backend.commit(
        revision,
        ChangeSet(
            create_indexes=(IndexSpec("priority"), IndexSpec("title", unique=True))
        ),
    )
    with backend.view() as view:
        assert view.lookup(
            "priority", IndexLookup.range(lower=Bound(5), upper=Bound(9, False))
        ) == {"b"}
        assert view.lookup("title", IndexLookup.equal("alpha")) == {"a"}
        assert [document["_id"] for document in view.fetch(["c", "a", "missing"])] == [
            "c",
            "a",
        ]
        assert {document["_id"] for document in view.scan()} == {"a", "b", "c"}
        stale_revision = view.revision

    with pytest.raises(WriteConflictError):
        backend.commit("0", ChangeSet(deletes=("a",)))
    with pytest.raises(DuplicateKeyError):
        backend.commit(stale_revision, ChangeSet(puts=(raw("d", "alpha", 10),)))

    with backend.view() as unchanged:
        assert unchanged.lookup("_id", IndexLookup.equal("d")) == set()
        assert unchanged.revision == stale_revision
    compacted = backend.compact()
    assert int(compacted) > int(stale_revision)
    with backend.view() as view:
        assert {document["_id"] for document in view.scan()} == {"a", "b", "c"}


def test_backend_views_remain_stable_across_commits(backend: StorageBackend) -> None:
    with backend.view() as view:
        revision = view.revision
    backend.commit(revision, ChangeSet(puts=(raw("a", "before", 1),)))
    old_view = backend.view()
    backend.commit(old_view.revision, ChangeSet(puts=(raw("a", "after", 2),)))
    assert [document["title"] for document in old_view.fetch(["a"])] == ["before"]
    old_view.close()
    with backend.view() as current:
        assert [document["title"] for document in current.fetch(["a"])] == ["after"]


def test_disk_reopen_uses_separate_content_and_index_files(tmp_path: Path) -> None:
    path = tmp_path / "notes.avldb"
    with Collection(Note, backend=DiskBackend(path)) as db:
        db.ensure_index("priority")
        db.insert_many([Note(title="a", priority=1), Note(title="b", priority=2)])
        db.update({"title": "a"}, {"$inc": {"priority": 3}})

    manifest = json.loads((path / "manifest.json").read_text())
    assert manifest["format"] == 2
    assert set(manifest["indexes"]) == {"_id", "priority"}
    assert list((path / "content").glob("*.jsonl"))
    index_directories = [
        directory for directory in (path / "indexes").iterdir() if directory.is_dir()
    ]
    assert len(index_directories) == 2
    assert all(list(directory.glob("*.jsonl")) for directory in index_directories)

    with Collection(Note, backend=DiskBackend(path)) as reopened:
        assert [
            note.title
            for note in reopened.find({"priority": {"$gte": 2}})
            .sort({"priority": 1})
            .all()
        ] == ["b", "a"]
        with reopened.backend.view() as view:
            assert "priority" in view.indexes


def test_disk_persists_multikey_nested_sparse_indexes_and_index_removal(
    tmp_path: Path,
) -> None:
    path = tmp_path / "index-kinds.avldb"
    with Collection(FlexibleRecord, backend=DiskBackend(path)) as db:
        db.insert_many(
            [
                {
                    "name": "one",
                    "code": "A",
                    "tags": ["red", "round"],
                    "metadata": {"rank": 3},
                },
                {"name": "two", "tags": ["blue"], "metadata": {"rank": 1}},
                {"name": "three", "tags": ["red"], "metadata": {"rank": 2}},
            ]
        )
        db.ensure_index("code", unique=True, sparse=True)
        db.ensure_index("tags")
        db.ensure_index("metadata.rank")

    with Collection(FlexibleRecord, backend=DiskBackend(path)) as reopened:
        assert {item.name for item in reopened.find({"tags": "red"}).all()} == {
            "one",
            "three",
        }
        assert [
            item.name for item in reopened.find({"metadata.rank": {"$lt": 2}}).all()
        ] == ["two"]
        assert [item.name for item in reopened.find({"code": "A"}).all()] == ["one"]
        reopened.remove_index("tags")

    with Collection(FlexibleRecord, backend=DiskBackend(path)) as reopened:
        with reopened.backend.view() as view:
            assert "tags" not in view.indexes
        # Removing an index affects planning, not query semantics; scan fallback
        # must still produce the same documents.
        assert {item.name for item in reopened.find({"tags": "red"}).all()} == {
            "one",
            "three",
        }


class CountingDiskBackend(DiskBackend):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.document_reads = 0

    def _read_location(self, state: object, document_id: str) -> dict[str, object] | None:  # type: ignore[override]
        self.document_reads += 1
        return super()._read_location(state, document_id)  # type: ignore[arg-type]


def test_disk_open_loads_indexes_not_documents_and_indexed_query_fetches_candidates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lazy.avldb"
    with Collection(Note, backend=DiskBackend(path)) as db:
        db.insert_many(
            Note(title=f"note-{value}", priority=value) for value in range(100)
        )
        db.ensure_index("priority")

    backend = CountingDiskBackend(path)
    with Collection(Note, backend=backend) as db:
        assert backend.document_reads == 0
        result = (
            db.find({"priority": {"$gte": 40, "$lt": 43}}).sort({"priority": 1}).all()
        )
        assert [note.priority for note in result] == [40, 41, 42]
        assert backend.document_reads == 3
        backend.document_reads = 0
        assert db.count({"title": {"$regex": __import__("re").compile("note")}}) == 100
        assert backend.document_reads == 100


def test_unsorted_cursor_iteration_streams_content_records(tmp_path: Path) -> None:
    path = tmp_path / "stream.avldb"
    with Collection(Note, backend=DiskBackend(path)) as db:
        db.insert_many(Note(title=f"note-{value}") for value in range(50))

    backend = CountingDiskBackend(path)
    with Collection(Note, backend=backend) as db:
        iterator = iter(db.find({}))
        first = next(iterator)
        assert first.title.startswith("note-")
        assert backend.document_reads == 1
        iterator.close()  # type: ignore[attr-defined]


def test_disk_orphan_segments_are_ignored_and_cleaned_on_open(tmp_path: Path) -> None:
    path = tmp_path / "orphan.avldb"
    with Collection(Note, backend=DiskBackend(path)) as db:
        db.insert(Note(title="committed"))
    orphan = path / "content" / "orphan.jsonl"
    orphan.write_text('{"op":"put","document":{"_id":"bad"}}\n')
    with Collection(Note, backend=DiskBackend(path)) as reopened:
        assert [note.title for note in reopened.find({}).all()] == ["committed"]
    assert not orphan.exists()


def test_disk_compaction_preserves_active_views_then_cleans_old_segments(
    tmp_path: Path,
) -> None:
    path = tmp_path / "views.avldb"
    backend = DiskBackend(path)
    backend.open()
    with backend.view() as initial:
        revision = initial.revision
    backend.commit(revision, ChangeSet(puts=(raw("a", "before", 1),)))
    old_view = backend.view()
    old_content = set((path / "content").glob("*.jsonl"))
    backend.compact()
    assert [document["title"] for document in old_view.fetch(["a"])] == ["before"]
    assert all(segment.exists() for segment in old_content)
    old_view.close()
    assert all(not segment.exists() for segment in old_content)
    with backend.view() as current:
        assert [document["title"] for document in current.fetch(["a"])] == ["before"]
    backend.close()


class FailingPublishDiskBackend(DiskBackend):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.fail_publication = False

    def _atomic_manifest(self, data: object) -> None:  # type: ignore[override]
        if self.fail_publication:
            raise BackendError("simulated manifest failure")
        super()._atomic_manifest(data)  # type: ignore[arg-type]


def test_failed_manifest_publication_keeps_content_and_indexes_invisible(
    tmp_path: Path,
) -> None:
    path = tmp_path / "atomic.avldb"
    backend = FailingPublishDiskBackend(path)
    db = Collection(Note, backend=backend)
    backend.fail_publication = True
    with pytest.raises(BackendError, match="simulated"):
        db.insert(Note(title="not committed", priority=10))
    backend.fail_publication = False
    assert db.count() == 0
    db.close()
    with Collection(Note, backend=DiskBackend(path)) as reopened:
        assert reopened.count() == 0


def test_second_disk_writer_is_rejected_and_lock_releases(tmp_path: Path) -> None:
    path = tmp_path / "locked.avldb"
    first = Collection(Note, backend=DiskBackend(path))
    with pytest.raises(DatabaseLockedError):
        Collection(Note, backend=DiskBackend(path))
    first.close()
    Collection(Note, backend=DiskBackend(path)).close()


def test_referenced_corrupt_index_segment_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.avldb"
    with Collection(Note, backend=DiskBackend(path)) as db:
        db.insert(Note(title="safe"))
    manifest = json.loads((path / "manifest.json").read_text())
    primary_segment = path / manifest["indexes"]["_id"]["segments"][0]
    primary_segment.write_bytes(b"not json\n")
    with pytest.raises(CorruptDataError):
        Collection(Note, backend=DiskBackend(path))


def test_legacy_single_file_is_explicitly_rejected(tmp_path: Path) -> None:
    path = tmp_path / "legacy.avldb"
    path.write_text("old format")
    with pytest.raises(BackendError, match="directory"):
        Collection(Note, backend=DiskBackend(path))
