"""Public exception hierarchy for avldb."""


class AvlDBError(Exception):
    """Base class for all database-specific errors."""


class ValidationError(AvlDBError):
    """A document or operation is invalid for the collection."""


class DuplicateKeyError(AvlDBError):
    """A unique index rejected a value."""

    def __init__(self, field: str, value: object) -> None:
        """Record the rejected index field and duplicate value."""

        self.field = field
        self.value = value
        super().__init__(f"Unique constraint violated for field {field!r}: {value!r}")


class QueryError(AvlDBError):
    """A query, projection, sort, or update expression is malformed."""


class CorruptDataError(AvlDBError):
    """A committed persistence record could not be decoded."""


class DatabaseLockedError(AvlDBError):
    """Another backend instance already owns the datastore."""


class ClosedDatabaseError(AvlDBError):
    """An operation was attempted after the collection was closed."""


class BackendError(AvlDBError):
    """A storage backend could not complete an operation."""


class WriteConflictError(BackendError):
    """A commit was based on a backend revision that is no longer current."""
