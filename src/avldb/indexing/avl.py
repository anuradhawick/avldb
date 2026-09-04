"""Rust-backed AVL secondary indexes."""

from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from typing import Iterable, Mapping

from rs_avl import AVLTree

from ..core.values import MISSING, get_path, normalize_key
from ..exceptions import DuplicateKeyError, ValidationError
from ..contracts import IndexSpec


@dataclass(slots=True)
class _Bucket:
    """One ordered index value and the document IDs sharing that value."""

    key: tuple[object, ...]
    value: object
    ids: set[str] = field(default_factory=set)


class Index:
    """An ordered value-to-document-ID map backed by ``rs_avl.AVLTree``."""

    def __init__(self, spec: IndexSpec) -> None:
        """Create an empty AVL bucket tree for ``spec``."""

        self.spec = spec
        self._tree = AVLTree(key="key")

    def values(self, document: Mapping[str, object]) -> list[object]:
        """Extract and deduplicate scalar index values from one document.

        Arrays become multikey entries. Sparse indexes omit missing fields, and
        object values are rejected because their index semantics are ambiguous.
        """

        value = get_path(document, self.spec.field)
        if value is MISSING and self.spec.sparse:
            return []
        values = value if isinstance(value, list) else [value]
        unique: dict[tuple[object, ...], object] = {}
        for item in values:
            if isinstance(item, Mapping):
                raise ValidationError(
                    f"cannot index an object or array of objects on {self.spec.field!r}"
                )
            key = normalize_key(item)
            unique[key] = item
        return list(unique.values())

    def add(self, document: Mapping[str, object]) -> None:
        """Add a document ID to every value bucket required by this index.

        A unique index raises ``DuplicateKeyError`` before accepting a second
        document into an existing bucket.
        """

        document_id = document["_id"]
        assert isinstance(document_id, str)
        for value in self.values(document):
            self.add_value(value, document_id)

    def remove(self, document: Mapping[str, object]) -> None:
        """Remove a document ID and discard buckets that become empty."""

        document_id = document["_id"]
        for value in self.values(document):
            self.remove_value(value, str(document_id))

    def add_value(self, value: object, document_id: str) -> None:
        """Add one already-extracted value and document ID to the tree."""

        key = normalize_key(value)
        bucket = self._tree.search_key(key)
        if bucket is None:
            bucket = _Bucket(key=key, value=value)
            if not self._tree.insert(bucket):
                raise RuntimeError("AVL bucket insertion failed")
        elif self.spec.unique and document_id not in bucket.ids:
            raise DuplicateKeyError(self.spec.field, value)
        bucket.ids.add(document_id)

    def remove_value(self, value: object, document_id: str) -> None:
        """Remove one document ID from an already-extracted value bucket."""

        key = normalize_key(value)
        bucket = self._tree.search_key(key)
        if bucket is None:
            return
        bucket.ids.discard(document_id)
        if not bucket.ids:
            self._tree.remove_key(key)

    def matching(self, value: object) -> set[str]:
        """Return a copy of IDs whose indexed value equals ``value``."""

        bucket = self._tree.search_key(normalize_key(value))
        return set() if bucket is None else set(bucket.ids)

    def matching_any(self, values: Iterable[object]) -> set[str]:
        """Return the union of exact-match IDs for all supplied values."""

        result: set[str] = set()
        for value in values:
            result.update(self.matching(value))
        return result

    def between(
        self,
        *,
        lower: object = MISSING,
        upper: object = MISSING,
        include_lower: bool = True,
        include_upper: bool = True,
    ) -> set[str]:
        """Return IDs in an ordered value range with configurable endpoints."""

        start = None if lower is MISSING else normalize_key(lower)
        end = None if upper is MISSING else normalize_key(upper)
        buckets = self._tree.range(
            start, end, include_start=include_lower, include_end=include_upper
        )
        result: set[str] = set()
        for bucket in buckets:
            result.update(bucket.ids)
        return result

    def all_ids(self) -> set[str]:
        """Return every document ID referenced by the index."""

        result: set[str] = set()
        for bucket in self._tree:
            result.update(bucket.ids)
        return result

    def entries(self) -> list[tuple[object, tuple[str, ...]]]:
        """Export values and sorted ID buckets for persistence or cloning."""

        return [(bucket.value, tuple(sorted(bucket.ids))) for bucket in self._tree]

    def clone(self) -> "Index":
        """Create an independent tree containing the same value buckets."""

        index = Index(self.spec)
        index._tree = pickle.loads(
            pickle.dumps(self._tree, protocol=pickle.HIGHEST_PROTOCOL)
        )
        return index

    @classmethod
    def build(
        cls, spec: IndexSpec, documents: Iterable[Mapping[str, object]]
    ) -> "Index":
        """Build and validate a complete index from an iterable of documents."""

        index = cls(spec)
        for document in documents:
            index.add(document)
        return index
