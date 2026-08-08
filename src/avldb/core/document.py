"""Pydantic document base type."""

from __future__ import annotations

import os
import secrets
import threading
import time
import uuid

from pydantic import BaseModel, ConfigDict, Field


_UUID7_RANDOM_BITS = 74
_UUID7_RANDOM_MASK = (1 << _UUID7_RANDOM_BITS) - 1
_id_lock = threading.Lock()
_last_timestamp_ms = -1
_last_random = -1
_last_process_id = os.getpid()


def new_document_id() -> str:
    """Return a monotonic UUIDv7 string.

    Values are strictly increasing within a process. The last timestamp is
    retained when the wall clock moves backwards, and the random portion is
    incremented when multiple IDs are requested in one millisecond.
    """

    global _last_process_id, _last_random, _last_timestamp_ms

    with _id_lock:
        process_id = os.getpid()
        if process_id != _last_process_id:
            # Do not retain a copied generator state after fork.
            _last_process_id = process_id
            _last_timestamp_ms = -1
            _last_random = -1

        timestamp_ms = time.time_ns() // 1_000_000
        if timestamp_ms > _last_timestamp_ms:
            random_bits = secrets.randbits(_UUID7_RANDOM_BITS)
        else:
            timestamp_ms = _last_timestamp_ms
            random_bits = _last_random + 1
            if random_bits > _UUID7_RANDOM_MASK:
                # Exhausting 2**74 IDs in one millisecond is not realistic;
                # advancing the logical millisecond keeps ordering strict.
                timestamp_ms += 1
                random_bits = secrets.randbits(_UUID7_RANDOM_BITS)

        _last_timestamp_ms = timestamp_ms
        _last_random = random_bits

        random_a = random_bits >> 62
        random_b = random_bits & ((1 << 62) - 1)
        value = (
            ((timestamp_ms & ((1 << 48) - 1)) << 80)
            | (0x7 << 76)
            | (random_a << 64)
            | (0b10 << 62)
            | random_b
        )
        return str(uuid.UUID(int=value))


class Document(BaseModel):
    """Base class for models stored in a :class:`Collection`.

    The immutable database identifier is exposed as ``_id``. Pydantic reserves
    underscore-prefixed field names, so an aliased backing field is used under
    the hood. Subclasses remain free to declare their own domain ``id`` field.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    avldb_id: str = Field(default_factory=new_document_id, alias="_id", frozen=True, repr=False)

    @property
    def _id(self) -> str:
        """The immutable database identifier."""

        return self.avldb_id
