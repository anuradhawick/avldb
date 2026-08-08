"""Manual indexed-versus-scan benchmark; no timing assertions."""

from time import perf_counter

from avldb import Collection, Document


class Row(Document):
    group: int
    value: int


def run(indexed: bool) -> float:
    db = Collection(Row)
    db.insert_many(Row(group=i % 100, value=i) for i in range(10_000))
    if indexed:
        db.ensure_index("group")
        db.ensure_index("value")
    started = perf_counter()
    for _ in range(500):
        db.find({"group": 42, "value": {"$gte": 5_000}}).all()
    return perf_counter() - started


if __name__ == "__main__":
    print({"scan_seconds": run(False), "indexed_seconds": run(True)})
