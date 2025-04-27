"""Exception hierarchy for sql-batcher.

This module defines the exception classes used throughout sql-batcher.
All exceptions inherit from SQLBatcherError to allow catching all sql-batcher
related exceptions with a single except clause.
"""


class SQLBatcherError(Exception):
    """Base exception class for all sql-batcher related errors."""


class BatchError(SQLBatcherError):
    """Base class for batch-related errors."""


class BatchSizeExceededError(BatchError):
    """Raised when a batch exceeds the maximum allowed size."""

    def __init__(self, batch_size: int, max_size: int, message: str = None):
        self.batch_size = batch_size
        self.max_size = max_size
        if message is None:
            message = (
                f"Batch size {batch_size} exceeds maximum allowed size of {max_size}"
            )
        super().__init__(message)


class InvalidQueryError(SQLBatcherError):
    """Raised when a query is invalid or malformed."""

    def __init__(self, query: str, message: str = None):
        self.query = query
        if message is None:
            message = f"Invalid query: {query}"
        super().__init__(message)


class AdapterError(SQLBatcherError):
    """Base class for adapter-related errors."""


class AdapterConnectionError(AdapterError):
    """Raised when there are issues connecting to the database."""

    def __init__(self, adapter_name: str, message: str = None):
        self.adapter_name = adapter_name
        if message is None:
            message = f"Failed to connect to database using {adapter_name} adapter"
        super().__init__(message)


class AdapterExecutionError(AdapterError):
    """Raised when there are issues executing queries through an adapter."""

    def __init__(self, adapter_name: str, query: str, message: str = None):
        self.adapter_name = adapter_name
        self.query = query
        if message is None:
            message = f"Failed to execute query using {adapter_name} adapter: {query}"
        super().__init__(message)


class ConfigurationError(SQLBatcherError):
    """Raised when there are issues with configuration settings."""


class RetryError(SQLBatcherError):
    """Base class for retry-related errors."""


class MaxRetriesExceededError(RetryError):
    """Raised when maximum number of retries is exceeded."""

    def __init__(self, max_retries: int, message: str = None):
        self.max_retries = max_retries
        if message is None:
            message = f"Maximum number of retries ({max_retries}) exceeded"
        super().__init__(message)


class TimeoutError(RetryError):
    """Raised when an operation times out."""

    def __init__(self, timeout_seconds: float, message: str = None):
        self.timeout_seconds = timeout_seconds
        if message is None:
            message = f"Operation timed out after {timeout_seconds} seconds"
        super().__init__(message)


class ValidationError(SQLBatcherError):
    """Raised when there is a validation error."""


class QueryCollectorError(SQLBatcherError):
    """Raised when there is an error in the query collector."""


class BatcherError(SQLBatcherError):
    """Raised when there is an error in the batcher."""


class ExecutorError(SQLBatcherError):
    """Raised when there is an error in the executor."""


class InsertMergerError(SQLBatcherError):
    """Raised when there is an error in the insert merger."""


class PluginError(SQLBatcherError):
    """Raised when there is an error in plugin operations."""
