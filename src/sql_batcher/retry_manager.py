"""Retry manager for SQL Batcher.

This module handles retry and timeout logic for SQL operations.
"""

import asyncio
import logging
from typing import Any, Callable, Optional, TypeVar, Union

from sql_batcher.exceptions import RetryError

T = TypeVar("T")


class RetryManager:
    """Manages retry and timeout logic for SQL operations."""

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: Optional[float] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the retry manager.

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            timeout: Operation timeout in seconds
            logger: Optional logger for retry events
        """
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._timeout = timeout
        self._logger = logger or logging.getLogger(__name__)

    async def execute_with_retry(
        self,
        operation: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute an operation with retry logic.

        Args:
            operation: Async operation to execute
            *args: Positional arguments for the operation
            **kwargs: Keyword arguments for the operation

        Returns:
            Result of the operation

        Raises:
            RetryError: If all retry attempts fail
        """
        last_error = None
        for attempt in range(self._max_retries + 1):
            try:
                if self._timeout is not None:
                    return await asyncio.wait_for(
                        operation(*args, **kwargs), timeout=self._timeout
                    )
                return await operation(*args, **kwargs)
            except asyncio.TimeoutError as e:
                last_error = e
                self._logger.warning(
                    f"Operation timed out after {self._timeout} seconds "
                    f"(attempt {attempt + 1}/{self._max_retries + 1})"
                )
            except Exception as e:
                last_error = e
                self._logger.warning(
                    f"Operation failed: {str(e)} "
                    f"(attempt {attempt + 1}/{self._max_retries + 1})"
                )

            if attempt < self._max_retries:
                await asyncio.sleep(self._retry_delay)

        raise RetryError(
            f"Operation failed after {self._max_retries + 1} attempts", last_error
        )

    def get_retry_info(self) -> dict[str, Any]:
        """Get retry configuration information.

        Returns:
            Dictionary of retry configuration
        """
        return {
            "max_retries": self._max_retries,
            "retry_delay": self._retry_delay,
            "timeout": self._timeout,
        }
