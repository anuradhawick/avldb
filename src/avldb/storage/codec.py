"""Tagged JSON codec shared by disk content and index segments."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Mapping

from ..core.values import MISSING
from ..exceptions import BackendError, CorruptDataError


def _json_value(value: object) -> object:
    """Encode supported values, datetimes, and the missing sentinel for JSON."""

    if value is MISSING:
        return {"$$missing": True}
    if isinstance(value, datetime):
        return {"$$date": value.isoformat()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _python_value(value: object) -> object:
    """Decode tagged JSON values used by content and index segments."""

    if isinstance(value, list):
        return [_python_value(item) for item in value]
    if isinstance(value, Mapping):
        if set(value) == {"$$missing"} and value["$$missing"] is True:
            return MISSING
        if set(value) == {"$$date"} and isinstance(value["$$date"], str):
            try:
                return datetime.fromisoformat(value["$$date"])
            except ValueError as error:
                raise CorruptDataError("invalid persisted datetime") from error
        return {str(key): _python_value(item) for key, item in value.items()}
    return value


def _encode_json(value: object) -> bytes:
    """Serialize one JSON-line value without a trailing newline."""

    try:
        return json.dumps(
            _json_value(value),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise BackendError("backend value cannot be serialized") from error


def _decode_json(value: bytes) -> object:
    """Decode one UTF-8 JSON value and restore tagged Python objects."""

    try:
        return _python_value(json.loads(value.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorruptDataError("invalid JSON in backend file") from error
