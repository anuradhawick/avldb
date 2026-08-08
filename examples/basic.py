"""Rich, runnable tour of avldb's typed CRUD, indexes, and disk backend."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import Field

from avldb import Collection, DiskBackend, Document


class Product(Document):
    """Application model with its own domain ID alongside avldb's ``_id``."""

    id: str
    name: str
    category: str
    price: float
    stock: int = 0
    tags: list[str] = Field(default_factory=list)
    details: dict[str, object] = Field(default_factory=dict)
    expires_at: datetime | None = None


def seed(products: Collection[Product]) -> None:
    """Create indexes and atomically insert a small product catalogue."""

    products.ensure_index("id", unique=True)
    products.ensure_index("category")
    products.ensure_index("price")
    products.ensure_index("tags")
    products.ensure_index("details.rating")
    products.ensure_index("expires_at", sparse=True, expire_after_seconds=0)
    products.insert_many(
        [
            Product(
                id="sku-keyboard",
                name="Mechanical Keyboard",
                category="hardware",
                price=129.0,
                stock=8,
                tags=["desk", "input", "featured"],
                details={"rating": 4.8, "layout": "75%"},
            ),
            Product(
                id="sku-mouse",
                name="Wireless Mouse",
                category="hardware",
                price=59.0,
                stock=20,
                tags=["desk", "input"],
                details={"rating": 4.5},
            ),
            Product(
                id="sku-course",
                name="Python Course",
                category="software",
                price=79.0,
                tags=["education", "featured"],
                details={"rating": 4.9},
            ),
            Product(
                id="sku-expired",
                name="Expired Voucher",
                category="software",
                price=10.0,
                expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            ),
        ]
    )


def demonstrate(products: Collection[Product]) -> None:
    """Run indexed reads, projection, updates, upsert, and deletion."""

    # TTL cleanup uses the expires_at range index; it does not scan every
    # document to discover records older than the cutoff.
    print("Removed expired products:", products.cleanup_expired())

    affordable_hardware = (
        products.find({"category": "hardware", "price": {"$gte": 50, "$lt": 100}})
        .sort({"price": 1})
        .all()
    )
    print("Affordable hardware:", [product.name for product in affordable_hardware])

    featured = products.find(
        {"tags": "featured"}, {"name": 1, "price": 1, "_id": 0}
    ).all()
    print("Featured projection:", featured)

    highly_rated = (
        products.find({"details.rating": {"$gte": 4.8}})
        .sort({"details.rating": -1})
        .all()
    )
    print("Highly rated:", [product.name for product in highly_rated])

    updated = products.update(
        {"id": "sku-keyboard"},
        {"$inc": {"stock": 5}, "$addToSet": {"tags": "restocked"}},
        return_updated=True,
    )
    print("Updated stock:", updated.documents[0].stock)

    upserted = products.update(
        {"id": "sku-monitor", "name": "4K Monitor", "category": "hardware"},
        {"$set": {"price": 399.0, "stock": 3, "tags": ["desk", "display"]}},
        upsert=True,
        return_updated=True,
    )
    print("Upserted:", upserted.documents[0].name)

    print("Products remaining:", products.count())


def main() -> None:
    """Create, reopen, query, and compact a temporary disk datastore."""

    with TemporaryDirectory() as directory:
        database_path = Path(directory) / "catalog.avldb"
        with Collection(Product, backend=DiskBackend(database_path)) as products:
            seed(products)
            demonstrate(products)
            products.compact()

        # DiskBackend reloads index files here, not every content document.
        with Collection(Product, backend=DiskBackend(database_path)) as products:
            product = products.find_one({"id": "sku-keyboard"})
            assert product is not None
            print("Reopened:", product.name, product._id)


if __name__ == "__main__":
    main()
