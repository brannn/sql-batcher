"""Retry and timeout functionality for sql-batcher.

This module provides utilities for retrying operations with configurable
backoff strategies and timeout handling.
"""

import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

from sql_batcher.exceptions import MaxRetriesExceededError, TimeoutError

T = TypeVar("T")


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        timeout: Optional[float] = None,
    ):
        """Initialize retry configuration.

        Args:
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            backoff_factor: Factor to increase delay by after each retry
            timeout: Maximum total time to spend retrying in seconds
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.timeout = timeout


def with_retry(
    func: Callable[..., T],
    retry_config: Optional[RetryConfig] = None,
) -> Callable[..., T]:
    """Decorator to add retry functionality to a function.

    Args:
        func: The function to retry
        retry_config: Configuration for retry behavior

    Returns:
        A wrapped function that will retry on failure
    """
    if retry_config is None:
        retry_config = RetryConfig()

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start_time = time.time()
        last_exception = None

        for attempt in range(retry_config.max_retries + 1):
            try:
                # Check timeout before attempting
                if retry_config.timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= retry_config.timeout:
                        raise TimeoutError(retry_config.timeout)

                return func(*args, **kwargs)

            except Exception as e:
                last_exception = e

                # Don't retry on last attempt
                if attempt == retry_config.max_retries:
                    break

                # Calculate delay with exponential backoff
                delay = min(
                    retry_config.initial_delay * (retry_config.backoff_factor**attempt),
                    retry_config.max_delay,
                )

                # Sleep before retrying
                time.sleep(delay)

        # If we get here, all retries failed
        raise MaxRetriesExceededError(retry_config.max_retries) from last_exception

    return wrapper
