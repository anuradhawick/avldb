"""Abstract snapshot backend contract and shared lookup helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Iterable, Iterator, Mapping

from ..contracts import ChangeSet, IndexLookup, IndexSpec
from ..core.values import MISSING
from ..indexing.avl import Index

DocumentData = dict[str, object]


class BackendView(ABC):
    """Immutable logical view of one backend revision.

    Views provide only the primitives needed by the query planner: streaming a
    collection, looking up candidate IDs, and fetching selected documents.
    Physical record locations and index caching remain backend details.
    """

    @property
    @abstractmethod
    def revision(self) -> str:
        """Return the optimistic-concurrency token for this view."""

    @property
    @abstractmethod
    def indexes(self) -> Mapping[str, IndexSpec]:
        """Return the indexes available at this revision, including ``_id``."""

    @abstractmethod
    def scan(self) -> Iterator[DocumentData]:
        """Stream every live document without requiring an index."""

    @abstractmethod
    def lookup(self, field: str, lookup: IndexLookup) -> set[str]:
        """Use one index to return matching document IDs."""

    @abstractmethod
    def fetch(self, document_ids: Iterable[str]) -> Iterator[DocumentData]:
        """Fetch selected live documents through backend-owned record locations."""

    @abstractmethod
    def close(self) -> None:
        """Release resources protecting this view's immutable files or state."""

    def __enter__(self) -> "BackendView":
        """Return the view for context-manager use."""

        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close the view without suppressing an active exception."""

        self.close()


class StorageBackend(ABC):
    """Storage and index boundary used by ``Collection``.

    Implementations choose record layout, index technology, caching, and
    durability. A commit must atomically apply content and every affected index
    relative to the supplied revision, or make none of the changes visible.
    """

    @abstractmethod
    def open(self) -> None:
        """Acquire backend resources and load index metadata."""

    @abstractmethod
    def view(self) -> BackendView:
        """Return an immutable view of the current committed revision."""

    @abstractmethod
    def commit(self, base_revision: str, changes: ChangeSet) -> str:
        """Atomically commit changes and return the resulting revision."""

    @abstractmethod
    def compact(self) -> str:
        """Consolidate backend-specific history and return the new revision."""

    @abstractmethod
    def close(self) -> None:
        """Release backend resources and writer ownership."""


def _lookup_index(index: Index, lookup: IndexLookup) -> set[str]:
    """Evaluate a backend-neutral lookup against an AVL index."""

    candidates: list[set[str]] = []
    if lookup.values is not None:
        candidates.append(index.matching_any(lookup.values))
    if lookup.lower is not None or lookup.upper is not None:
        candidates.append(
            index.between(
                lower=MISSING if lookup.lower is None else lookup.lower.value,
                upper=MISSING if lookup.upper is None else lookup.upper.value,
                include_lower=True if lookup.lower is None else lookup.lower.inclusive,
                include_upper=True if lookup.upper is None else lookup.upper.inclusive,
            )
        )
    if not candidates:
        return set()
    result = candidates[0]
    for candidate in candidates[1:]:
        result &= candidate
    return result


def _copy_document(document: Mapping[str, object]) -> DocumentData:
    """Return an isolated mutable copy of a canonical document mapping."""

    return deepcopy(dict(document))
