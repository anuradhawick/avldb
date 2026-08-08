"""Internal document/path and value helpers."""

from __future__ import annotations

import math
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Mapping, MutableMapping

from ..exceptions import QueryError, ValidationError


class _Missing:
    """Sentinel type distinguishing an absent field from an explicit ``None``."""

    def __repr__(self) -> str:
        """Return the stable diagnostic name used for absent document fields."""

        return "MISSING"


MISSING = _Missing()


def get_path(value: object, path: str | list[str]) -> object:
    """Resolve a dot path, recursively projecting paths across array elements.

    ``MISSING`` is returned when a segment does not exist. A numeric segment
    selects one array position; a non-numeric segment maps the remaining path
    across every array item, matching NeDB's nested-array behavior.
    """

    parts = path.split(".") if isinstance(path, str) else path
    if not parts:
        return value
    if value is None or value is MISSING:
        return MISSING
    head, *tail = parts
    if isinstance(value, Mapping):
        if head not in value:
            return MISSING
        return get_path(value[head], tail)
    if isinstance(value, (list, tuple)):
        if head.isdigit():
            index = int(head)
            if index >= len(value):
                return MISSING
            return get_path(value[index], tail)
        return [get_path(item, parts) for item in value]
    return MISSING


def set_path(document: MutableMapping[str, Any], path: str, value: object) -> None:
    """Deep-copy ``value`` into ``document`` at a validated dot path.

    Missing intermediate objects are created. Traversing an existing scalar or
    using an operator-like path segment raises ``QueryError``.
    """

    parts = path.split(".")
    if any(not part or part.startswith("$") for part in parts):
        raise QueryError(f"invalid update path: {path!r}")
    current = document
    for part in parts[:-1]:
        child = current.get(part)
        if child is None:
            child = {}
            current[part] = child
        if not isinstance(child, MutableMapping):
            raise QueryError(f"cannot traverse non-object field in path {path!r}")
        current = child
    current[parts[-1]] = deepcopy(value)


def unset_path(document: MutableMapping[str, Any], path: str) -> None:
    """Remove a dot-path value, doing nothing when any segment is absent."""

    parts = path.split(".")
    current: object = document
    for part in parts[:-1]:
        if not isinstance(current, MutableMapping) or part not in current:
            return
        current = current[part]
    if isinstance(current, MutableMapping):
        current.pop(parts[-1], None)


def validate_storable(value: object, *, path: str = "document") -> None:
    """Validate the JSON-like subset on which queries and indexes are defined."""

    if value is None or isinstance(value, (str, bool, datetime)):
        return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValidationError(f"{path} contains a non-finite number")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_storable(item, path=f"{path}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValidationError(f"{path} contains a non-string key")
            if key.startswith("$") or "." in key:
                raise ValidationError(f"invalid field name {key!r} at {path}")
            validate_storable(item, path=f"{path}.{key}")
        return
    raise ValidationError(f"unsupported value at {path}: {type(value).__name__}")


def normalize_key(value: object) -> tuple[object, ...]:
    """Create a total-order key that keeps Python bool and number distinct."""

    if value is MISSING:
        return (-1,)
    if value is None:
        return (0,)
    if isinstance(value, bool):
        return (3, int(value))
    if isinstance(value, (int, float)):
        return (1, value)
    if isinstance(value, str):
        return (2, value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return (4, value.astimezone(timezone.utc).timestamp())
    if isinstance(value, (list, tuple)):
        return (5, tuple(normalize_key(item) for item in value))
    if isinstance(value, Mapping):
        return (6, tuple((key, normalize_key(value[key])) for key in sorted(value)))
    raise TypeError(f"unsupported indexed value: {type(value).__name__}")


def comparable(left: object, right: object) -> bool:
    """Return whether values support NeDB-style ordered comparison.

    Numeric types compare with each other, while booleans, strings, and
    datetimes compare only with values from their own logical type.
    """

    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool)
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return True
    if isinstance(left, str) and isinstance(right, str):
        return True
    if isinstance(left, datetime) and isinstance(right, datetime):
        return True
    return False
