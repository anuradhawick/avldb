from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import Field

from avldb import Collection, Document, DuplicateKeyError, QueryError, ValidationError


class Planet(Document):
    name: str
    system: str
    inhabited: bool = False
    order: int = 0
    satellites: list[str] = Field(default_factory=list)
    facts: dict[str, object] = Field(default_factory=dict)


@pytest.fixture
def planets() -> Collection[Planet]:
    collection = Collection(Planet)
    collection.insert_many(
        [
            Planet(
                name="Mars", system="solar", order=4, satellites=["Phobos", "Deimos"]
            ),
            Planet(
                name="Earth",
                system="solar",
                order=3,
                inhabited=True,
                facts={"life": {"eyes": True}},
            ),
            Planet(name="Jupiter", system="solar", order=5),
            Planet(name="Omicron Persei 8", system="futurama", order=8, inhabited=True),
        ]
    )
    return collection


def names(results: object) -> set[str]:
    return {item.name for item in results}  # type: ignore[union-attr]


def test_generated_ids_are_monotonic_uuid7() -> None:
    generated = [Planet(name=str(index), system="test")._id for index in range(1_000)]
    assert generated == sorted(generated)
    assert len(set(generated)) == len(generated)
    assert all(uuid.UUID(value).version == 7 for value in generated)


def test_document_id_alias_validation_and_copy_isolation() -> None:
    planet = Planet.model_validate({"_id": "p1", "name": "Earth", "system": "solar"})
    assert planet._id == "p1"
    assert planet.model_dump(by_alias=True)["_id"] == "p1"
    with pytest.raises(Exception):
        planet._id = "changed"  # type: ignore[misc]

    db = Collection(Planet)
    stored = db.insert(planet)
    stored.satellites.append("Moon")
    assert db.find_one({"_id": "p1"}).satellites == []  # type: ignore[union-attr]
    with pytest.raises(ValidationError):
        db.insert({"name": "missing system"})


def test_application_model_can_define_its_own_id() -> None:
    class ExternalRecord(Document):
        id: int
        value: str

    db = Collection(ExternalRecord)
    stored = db.insert(ExternalRecord(id=42, value="answer"))
    assert stored.id == 42
    assert uuid.UUID(stored._id).version == 7
    assert db.find_one({"id": 42})._id == stored._id  # type: ignore[union-attr]
    dumped = stored.model_dump(by_alias=True)
    assert dumped["id"] == 42 and dumped["_id"] == stored._id


def test_basic_nested_array_and_comparison_queries(planets: Collection[Planet]) -> None:
    assert names(planets.find({"system": "solar"}).all()) == {
        "Mars",
        "Earth",
        "Jupiter",
    }
    assert names(planets.find({"system": {"$eq": "solar"}}).all()) == {
        "Mars",
        "Earth",
        "Jupiter",
    }
    assert names(planets.find({"order": {"$gt": 4, "$lte": 8}}).all()) == {
        "Jupiter",
        "Omicron Persei 8",
    }
    assert names(planets.find({"name": {"$in": ["Earth", "Mars"]}}).all()) == {
        "Earth",
        "Mars",
    }
    assert names(planets.find({"name": {"$nin": ["Earth", "Mars"]}}).all()) == {
        "Jupiter",
        "Omicron Persei 8",
    }
    assert names(planets.find({"satellites": "Phobos"}).all()) == {"Mars"}
    assert names(planets.find({"satellites": {"$eq": "Phobos"}}).all()) == {"Mars"}
    assert names(planets.find({"satellites": ["Phobos", "Deimos"]}).all()) == {"Mars"}
    assert names(planets.find({"satellites": {"$eq": ["Phobos", "Deimos"]}}).all()) == {
        "Mars"
    }
    assert names(planets.find({"satellites": {"$size": 2}}).all()) == {"Mars"}
    assert names(planets.find({"facts.life.eyes": True}).all()) == {"Earth"}
    assert names(planets.find({"facts.life": {"$exists": True}}).all()) == {"Earth"}
    assert names(planets.find({"name": re.compile("ar", re.I)}).all()) == {
        "Mars",
        "Earth",
    }
    assert names(planets.find({"name": {"$regex": re.compile("^J")}}).all()) == {
        "Jupiter"
    }


def test_logical_elem_match_and_where_queries() -> None:
    class Survey(Document):
        label: str
        readings: list[dict[str, object]]

    db = Collection(Survey)
    db.insert_many(
        [
            Survey(
                label="good",
                readings=[
                    {"kind": "temperature", "value": 22},
                    {"kind": "humidity", "value": 40},
                ],
            ),
            Survey(label="bad", readings=[{"kind": "temperature", "value": 3}]),
        ]
    )
    assert db.find_one({"readings": {"$elemMatch": {"kind": "temperature", "value": {"$gt": 20}}}}).label == "good"  # type: ignore[union-attr]
    assert {
        item.label
        for item in db.find({"$or": [{"label": "good"}, {"label": "unknown"}]}).all()
    } == {"good"}
    assert {
        item.label
        for item in db.find(
            {"$and": [{"label": {"$ne": "bad"}}, {"readings.value": {"$gte": 20}}]}
        ).all()
    } == {"good"}
    assert {item.label for item in db.find({"$not": {"label": "bad"}}).all()} == {
        "good"
    }
    assert {
        item.label
        for item in db.find(
            {"$where": lambda document: len(document["readings"]) > 1}
        ).all()
    } == {"good"}


def test_sort_paging_and_projection(planets: Collection[Planet]) -> None:
    result = (
        planets.find({"system": "solar"}).sort({"order": -1}).skip(1).limit(1).all()
    )
    assert names(result) == {"Mars"}
    projected = planets.find({"name": "Earth"}, {"name": 1, "_id": 0}).first()
    assert projected == {"name": "Earth"}
    excluded = planets.find({"name": "Earth"}, {"facts": 0}).first()
    assert (
        isinstance(excluded, dict)
        and "facts" not in excluded
        and excluded["name"] == "Earth"
    )
    id_only = planets.find({"name": "Earth"}, {"_id": 1}).first()
    assert isinstance(id_only, dict) and set(id_only) == {"_id"}
    with pytest.raises(QueryError):
        planets.find({}, {"name": 1, "system": 0}).all()


def test_update_modifiers_replacement_upsert_and_remove(
    planets: Collection[Planet],
) -> None:
    result = planets.update(
        {"name": "Mars"},
        {
            "$inc": {"order": 1},
            "$set": {"facts.color": "red"},
            "$push": {"satellites": {"$each": ["A", "B"], "$slice": -3}},
        },
        return_updated=True,
    )
    assert result.count == 1 and result.documents[0].order == 5
    assert result.documents[0].satellites == ["Deimos", "A", "B"]

    planets.update(
        {"name": "Mars"}, {"$addToSet": {"satellites": {"$each": ["A", "C"]}}}
    )
    planets.update({"name": "Mars"}, {"$pull": {"satellites": re.compile("^[AB]$")}})
    planets.update({"name": "Mars"}, {"$pop": {"satellites": -1}})
    planets.update({"name": "Mars"}, {"$push": {"satellites": {"$slice": 1}}})
    mars = planets.find_one({"name": "Mars"})
    assert mars.satellites == ["C"]  # type: ignore[union-attr]

    replaced = planets.update(
        {"name": "Jupiter"},
        {"name": "Jove", "system": "solar", "order": 5},
        return_updated=True,
    )
    assert replaced.documents[0].name == "Jove"
    upserted = planets.update(
        {"name": "Venus", "system": "solar"},
        {"$set": {"order": 2}},
        upsert=True,
        return_updated=True,
    )
    assert upserted.upserted and upserted.documents[0].name == "Venus"
    assert planets.remove({"system": "solar"}, multi=True) == 4


def test_ttl_cleanup() -> None:
    class Token(Document):
        name: str
        expires_at: datetime | None = None

    db = Collection(Token)
    db.ensure_index("expires_at", expire_after_seconds=0)
    db.insert_many(
        [
            Token(
                name="old", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
            ),
            Token(
                name="new", expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
            ),
            Token(name="forever"),
        ]
    )
    assert names(db.find({}).all()) == {"new", "forever"}


def test_unique_index_rolls_back_bulk_insert() -> None:
    db = Collection(Planet)
    db.ensure_index("name", unique=True)
    with pytest.raises(DuplicateKeyError):
        db.insert_many(
            [
                Planet(name="same", system="one"),
                Planet(name="same", system="two"),
            ]
        )
    assert db.count() == 0
