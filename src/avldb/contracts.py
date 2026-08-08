"""Shared public value types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Mapping, TypeVar

from .core.document import Document

DocumentT = TypeVar("DocumentT", bound=Document)


@dataclass(frozen=True, slots=True)
class IndexSpec:
    """Definition of a maintained collection index."""

    field: str
    unique: bool = False
    sparse: bool = False
    expire_after_seconds: float | None = None

    def __post_init__(self) -> None:
        """Reject invalid field names and negative TTL durations."""

        if not self.field or self.field.startswith("$"):
            raise ValueError("index field must be a non-empty document path")
        if self.expire_after_seconds is not None and self.expire_after_seconds < 0:
            raise ValueError("expire_after_seconds must be non-negative")


@dataclass(frozen=True, slots=True)
class UpdateResult(Generic[DocumentT]):
    """Outcome of an update or upsert operation."""

    count: int
    documents: tuple[DocumentT, ...] = ()
    upserted: bool = False


@dataclass(frozen=True, slots=True)
class Bound:
    """One inclusive or exclusive endpoint of an index range lookup."""

    value: object
    inclusive: bool = True


@dataclass(frozen=True, slots=True)
class IndexLookup:
    """Backend-neutral equality or ordered-range index request.

    ``values`` requests the union of exact matches. Bounds may be supplied
    independently or together. A planner can intersect multiple lookups when a
    field expression contains both membership and range constraints.
    """

    values: tuple[object, ...] | None = None
    lower: Bound | None = None
    upper: Bound | None = None

    def __post_init__(self) -> None:
        """Require the lookup to contain exact values or at least one bound."""

        if self.values is None and self.lower is None and self.upper is None:
            raise ValueError("an index lookup requires values or a range bound")

    @classmethod
    def equal(cls, value: object) -> "IndexLookup":
        """Create a lookup for one exact value."""

        return cls(values=(value,))

    @classmethod
    def any(cls, values: tuple[object, ...]) -> "IndexLookup":
        """Create a lookup returning the union of several exact values."""

        return cls(values=values)

    @classmethod
    def range(
        cls, *, lower: Bound | None = None, upper: Bound | None = None
    ) -> "IndexLookup":
        """Create an ordered lookup between optional endpoints."""

        return cls(lower=lower, upper=upper)


@dataclass(frozen=True, slots=True)
class ChangeSet:
    """One atomic backend mutation prepared by the collection layer."""

    puts: tuple[Mapping[str, object], ...] = ()
    deletes: tuple[str, ...] = ()
    create_indexes: tuple[IndexSpec, ...] = ()
    drop_indexes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Reject contradictory index metadata changes."""

        created = {spec.field for spec in self.create_indexes}
        dropped = set(self.drop_indexes)
        if created & dropped:
            raise ValueError("an index cannot be created and dropped in one change set")

    @property
    def empty(self) -> bool:
        """Return whether this change set contains no mutations."""

        return not (
            self.puts or self.deletes or self.create_indexes or self.drop_indexes
        )
