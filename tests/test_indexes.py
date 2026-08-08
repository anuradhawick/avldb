from __future__ import annotations

from pydantic import ConfigDict, Field
import re
import pytest

from avldb import Collection, Document, DuplicateKeyError, ValidationError


class Item(Document):
    group: str | None = None
    score: int
    flags: list[str] = Field(default_factory=list)
    nested: dict[str, int] = Field(default_factory=dict)


def test_sparse_multikey_nested_and_intersected_indexes() -> None:
    db = Collection(Item)
    db.insert_many(
        [
            Item(group="a", score=1, flags=["red", "round"], nested={"rank": 3}),
            Item(group="a", score=5, flags=["blue"], nested={"rank": 2}),
            Item(score=8, flags=["red"], nested={"rank": 1}),
        ]
    )
    db.ensure_index("group", sparse=True)
    db.ensure_index("score")
    db.ensure_index("flags")
    db.ensure_index("nested.rank")

    assert [
        item.score for item in db.find({"group": "a", "score": {"$gte": 3}}).all()
    ] == [5]
    assert {item.score for item in db.find({"flags": "red"}).all()} == {1, 8}
    assert [item.score for item in db.find({"nested.rank": {"$lt": 2}}).all()] == [8]
    assert {
        item.score for item in db.find({"$or": [{"score": 1}, {"score": 8}]}).all()
    } == {1, 8}
    assert {
        item.score for item in db.find({"group": {"$in": [re.compile("^a$")]}}).all()
    } == {1, 5}


def test_bool_and_number_are_distinct_index_keys() -> None:
    class Typed(Document):
        value: bool | int

    db = Collection(Typed)
    db.insert_many([Typed(value=True), Typed(value=1)])
    db.ensure_index("value", unique=True)
    assert len(db.find({"value": True}).all()) == 1
    assert len(db.find({"value": 1}).all()) == 1


def test_unique_sparse_index_and_failed_creation_rollback() -> None:
    class FlexibleItem(Document):
        model_config = ConfigDict(populate_by_name=True, extra="allow")
        score: int

    db = Collection(FlexibleItem)
    db.insert_many([FlexibleItem(score=1), FlexibleItem(score=2)])
    with pytest.raises(DuplicateKeyError):
        db.ensure_index("group", unique=True)
    assert db.count() == 2
    db.ensure_index("group", unique=True, sparse=True)
    db.insert({"group": "a", "score": 3})
    with pytest.raises(DuplicateKeyError):
        db.insert({"group": "a", "score": 4})
    assert db.count() == 3


def test_indexing_objects_is_rejected_without_changing_collection() -> None:
    db = Collection(Item)
    db.insert(Item(score=1, nested={"rank": 1}))
    with pytest.raises(ValidationError):
        db.ensure_index("nested")
    assert db.count() == 1
