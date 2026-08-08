"""NeDB-style query, projection, sorting, and update primitives."""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime
from functools import cmp_to_key
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Pattern, Sequence

from .values import MISSING, comparable, get_path, normalize_key, set_path, unset_path
from ..exceptions import QueryError

Query = Mapping[str, object]
Projection = Mapping[str, int | bool]
SortSpec = Mapping[str, int]


def values_equal(left: object, right: object) -> bool:
    """Compare supported values with NeDB semantics and strict type handling.

    Objects and arrays compare deeply, datetimes compare by normalized instant,
    and booleans remain distinct from Python's numerically equal ``0``/``1``.
    """

    if left is MISSING or right is MISSING:
        return False
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if isinstance(left, datetime) or isinstance(right, datetime):
        return (
            isinstance(left, datetime)
            and isinstance(right, datetime)
            and normalize_key(left) == normalize_key(right)
        )
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return set(left) == set(right) and all(
            values_equal(left[k], right[k]) for k in left
        )
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(
            values_equal(a, b) for a, b in zip(left, right)
        )
    return type(left) is type(right) and left == right


def _compare(left: object, right: object, operator: str) -> bool:
    """Evaluate one ordered comparison after checking type compatibility."""

    if not comparable(left, right):
        return False
    lkey = normalize_key(left)
    rkey = normalize_key(right)
    return {
        "$lt": lkey < rkey,
        "$lte": lkey <= rkey,
        "$gt": lkey > rkey,
        "$gte": lkey >= rkey,
    }[operator]


def _is_operator_expression(value: object) -> bool:
    """Return whether a mapping contains at least one dollar-prefixed key."""

    return isinstance(value, Mapping) and any(str(key).startswith("$") for key in value)


def _match_operator(value: object, operator: str, operand: object) -> bool:
    """Evaluate one field-level query operator against a resolved value.

    This handles scalar comparisons plus membership, existence, regex, size,
    and element-match expressions, raising ``QueryError`` for invalid operands.
    """

    if operator in {"$lt", "$lte", "$gt", "$gte"}:
        return _compare(value, operand, operator)
    if operator == "$ne":
        return value is MISSING or not values_equal(value, operand)
    if operator in {"$in", "$nin"}:
        if not isinstance(operand, (list, tuple)):
            raise QueryError(f"{operator} requires an array")
        found = any(
            (
                bool(item.search(value))
                if isinstance(item, re.Pattern) and isinstance(value, str)
                else values_equal(value, item)
            )
            for item in operand
        )
        return found if operator == "$in" else not found
    if operator == "$exists":
        return (value is not MISSING) is bool(operand)
    if operator == "$regex":
        if not isinstance(operand, re.Pattern):
            raise QueryError("$regex requires a compiled regular expression")
        return isinstance(value, str) and operand.search(value) is not None
    if operator == "$size":
        if not isinstance(operand, int) or isinstance(operand, bool):
            raise QueryError("$size requires an integer")
        return isinstance(value, (list, tuple)) and len(value) == operand
    if operator == "$elemMatch":
        if not isinstance(value, (list, tuple)) or not isinstance(operand, Mapping):
            return False
        return any(
            (
                match_document(item, operand)
                if isinstance(item, Mapping)
                else _match_field(item, operand)
            )
            for item in value
        )
    raise QueryError(f"unknown query operator {operator!r}")


def _match_field(value: object, condition: object) -> bool:
    """Match a field value against a literal, regex, or operator expression.

    Scalar conditions applied to arrays match any element, while literal array
    conditions use ordered deep equality and array-specific operators inspect
    the array itself.
    """

    if isinstance(condition, re.Pattern):
        if isinstance(value, (list, tuple)):
            return any(_match_field(item, condition) for item in value)
        return isinstance(value, str) and condition.search(value) is not None

    if _is_operator_expression(condition):
        assert isinstance(condition, Mapping)
        array_specific = any(op in condition for op in ("$size", "$elemMatch"))
        if isinstance(value, (list, tuple)) and not array_specific:
            # Negative operators must hold for the array as a whole; positive
            # operators match when any element satisfies the expression.
            if set(condition).issubset({"$ne", "$nin", "$exists"}):
                return all(
                    all(
                        _match_operator(item, str(op), arg)
                        for op, arg in condition.items()
                    )
                    for item in value
                )
            return any(
                all(
                    _match_operator(item, str(op), arg) for op, arg in condition.items()
                )
                for item in value
            )
        return all(
            _match_operator(value, str(op), arg) for op, arg in condition.items()
        )

    if isinstance(value, (list, tuple)):
        if isinstance(condition, (list, tuple)):
            return values_equal(value, condition)
        return any(_match_field(item, condition) for item in value)
    return values_equal(value, condition)


def match_document(document: object, query: Query) -> bool:
    """Return whether a document satisfies every field and logical expression.

    Dot paths are resolved lazily and logical operators recurse into this same
    matcher. ``$where`` receives a deep copy to prevent query code mutating the
    stored document.
    """

    if not isinstance(query, Mapping):
        raise QueryError("query must be a mapping")
    if not isinstance(document, Mapping):
        return False
    for field, condition in query.items():
        if field == "$or" or field == "$and":
            if not isinstance(condition, (list, tuple)) or not all(
                isinstance(item, Mapping) for item in condition
            ):
                raise QueryError(f"{field} requires an array of query mappings")
            matches = [match_document(document, item) for item in condition]
            if (field == "$or" and not any(matches)) or (
                field == "$and" and not all(matches)
            ):
                return False
        elif field == "$not":
            if not isinstance(condition, Mapping):
                raise QueryError("$not requires a query mapping")
            if match_document(document, condition):
                return False
        elif field == "$where":
            if not callable(condition):
                raise QueryError("$where requires a callable")
            if not bool(condition(deepcopy(document))):
                return False
        elif field.startswith("$"):
            raise QueryError(f"unknown logical operator {field!r}")
        elif not _match_field(get_path(document, field), condition):
            return False
    return True


def apply_projection(
    document: Mapping[str, object], projection: Projection
) -> dict[str, object]:
    """Create an inclusion or exclusion projection of a document.

    Inclusion and exclusion cannot be mixed except for ``_id``, mirroring the
    behavior users expect from NeDB and MongoDB-style projections.
    """

    if not projection:
        return deepcopy(dict(document))
    include = {
        field
        for field, enabled in projection.items()
        if bool(enabled) and field != "_id"
    }
    exclude = {
        field
        for field, enabled in projection.items()
        if not bool(enabled) and field != "_id"
    }
    if include and exclude:
        raise QueryError("cannot mix inclusion and exclusion projection")
    id_only_inclusion = bool(projection.get("_id")) and not exclude and not include
    if include or id_only_inclusion:
        result: dict[str, object] = {}
        for field in include:
            value = get_path(document, field)
            if value is not MISSING:
                set_path(result, field, value)
        if projection.get("_id", 1) and "_id" in document:
            result["_id"] = deepcopy(document["_id"])
        return result
    result = deepcopy(dict(document))
    for field in exclude:
        unset_path(result, field)
    if not projection.get("_id", 1):
        result.pop("_id", None)
    return result


def sort_documents(
    documents: list[dict[str, object]], spec: SortSpec
) -> list[dict[str, object]]:
    """Return documents sorted by each path and direction in specification order."""

    for direction in spec.values():
        if direction not in (-1, 1):
            raise QueryError("sort directions must be 1 or -1")

    def compare(left: Mapping[str, object], right: Mapping[str, object]) -> int:
        """Compare two documents using normalized values for each sort field."""

        for field, direction in spec.items():
            lkey = normalize_key(get_path(left, field))
            rkey = normalize_key(get_path(right, field))
            if lkey < rkey:
                return -direction
            if lkey > rkey:
                return direction
        return 0

    return sorted(documents, key=cmp_to_key(compare))


def _modifier_paths(value: object, operator: str) -> Mapping[str, object]:
    """Validate and return the path mapping required by an update operator."""

    if not isinstance(value, Mapping):
        raise QueryError(f"{operator} requires a mapping")
    return value


def apply_update(
    document: Mapping[str, object], update: Mapping[str, object]
) -> dict[str, object]:
    """Apply a replacement or modifier update to a copied document.

    Supported scalar and array modifiers follow NeDB behavior. The caller must
    subsequently validate the returned mapping against its Pydantic model; this
    function enforces expression shape and database-ID immutability.
    """

    if not isinstance(update, Mapping) or not update:
        raise QueryError("update must be a non-empty mapping")
    operators = [str(key).startswith("$") for key in update]
    if any(operators) and not all(operators):
        raise QueryError("cannot mix update operators and replacement fields")
    old_id = document.get("_id")
    if not any(operators):
        result = deepcopy(dict(update))
        if "_id" in result and result["_id"] != old_id:
            raise QueryError("cannot change a document id")
        result["_id"] = old_id
        return result

    result: dict[str, Any] = deepcopy(dict(document))
    for operator, raw_changes in update.items():
        changes = _modifier_paths(raw_changes, operator)
        if operator == "$set":
            for path, value in changes.items():
                if path == "_id" and value != old_id:
                    raise QueryError("cannot change a document id")
                set_path(result, path, value)
        elif operator == "$unset":
            for path in changes:
                if path == "_id":
                    raise QueryError("cannot remove a document id")
                unset_path(result, path)
        elif operator in {"$inc", "$min", "$max"}:
            for path, operand in changes.items():
                current = get_path(result, path)
                if operator == "$inc":
                    if not isinstance(operand, (int, float)) or isinstance(
                        operand, bool
                    ):
                        raise QueryError("$inc operands must be numeric")
                    if current is MISSING:
                        set_path(result, path, operand)
                    elif isinstance(current, (int, float)) and not isinstance(
                        current, bool
                    ):
                        set_path(result, path, current + operand)
                    else:
                        raise QueryError("cannot apply $inc to a non-number")
                elif current is MISSING or (
                    comparable(current, operand)
                    and (
                        (
                            operator == "$min"
                            and normalize_key(operand) < normalize_key(current)
                        )
                        or (
                            operator == "$max"
                            and normalize_key(operand) > normalize_key(current)
                        )
                    )
                ):
                    set_path(result, path, operand)
        elif operator in {"$push", "$addToSet", "$pop", "$pull"}:
            for path, operand in changes.items():
                current = get_path(result, path)
                if current is MISSING and operator in {"$push", "$addToSet"}:
                    current = []
                    set_path(result, path, current)
                    current = get_path(result, path)
                if not isinstance(current, list):
                    raise QueryError(f"cannot apply {operator} to a non-array")
                if operator == "$push":
                    if isinstance(operand, Mapping) and (
                        "$each" in operand or "$slice" in operand
                    ):
                        if set(operand) - {"$each", "$slice"}:
                            raise QueryError(
                                "$push only supports $each and $slice options"
                            )
                        items = operand.get("$each", [])
                        if not isinstance(items, list):
                            raise QueryError("$each requires an array")
                        current.extend(deepcopy(items))
                        if "$slice" in operand:
                            size = operand["$slice"]
                            if not isinstance(size, int) or isinstance(size, bool):
                                raise QueryError("$slice requires an integer")
                            current[:] = current[:size] if size >= 0 else current[size:]
                    else:
                        current.append(deepcopy(operand))
                elif operator == "$addToSet":
                    if (
                        isinstance(operand, Mapping)
                        and "$each" in operand
                        and set(operand) != {"$each"}
                    ):
                        raise QueryError("$addToSet only supports the $each option")
                    items = (
                        operand.get("$each")
                        if isinstance(operand, Mapping) and "$each" in operand
                        else [operand]
                    )
                    if not isinstance(items, list):
                        raise QueryError("$each requires an array")
                    for item in items:
                        if not any(
                            values_equal(item, existing) for existing in current
                        ):
                            current.append(deepcopy(item))
                elif operator == "$pop":
                    if operand not in (-1, 1):
                        raise QueryError("$pop requires 1 or -1")
                    if current:
                        current.pop(0 if operand == -1 else -1)
                else:
                    current[:] = [
                        item for item in current if not _match_field(item, operand)
                    ]
        else:
            raise QueryError(f"unknown update operator {operator!r}")
    if result.get("_id") != old_id:
        raise QueryError("cannot change a document id")
    return result


def query_seed(query: Mapping[str, object]) -> dict[str, object]:
    """Strip operators from an upsert query to produce its initial document."""

    result: dict[str, object] = {}
    for field, value in query.items():
        if field.startswith("$") or _is_operator_expression(value):
            continue
        set_path(result, field, value)
    return result
