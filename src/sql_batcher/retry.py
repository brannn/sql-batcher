"""
Retry mechanism for SQL Batcher.

This module provides retry functionality for SQL operations that may fail
due to transient errors such as network issues or database timeouts.
"""

import logging
import random
import time
from functools import wraps
from typing import Any, Callable, List, Optional, Type, TypeVar, cast

import asyncio

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])
AsyncF = TypeVar("AsyncF", bound=Callable[..., Any])

logger = logging.getLogger(__name__)


class RetryConfig:
    """
    Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Factor by which to increase delay after each attempt
        jitter: Whether to add random jitter to delay
        retryable_exceptions: List of exception types that should trigger a retry
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 10.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Optional[List[Type[Exception]]] = None,
    ) -> None:
        """Initialize retry configuration."""
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or []

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate the delay for a retry attempt.

        Args:
            attempt: Current attempt number (0-based)

        Returns:
            Delay in seconds
        """
        delay = self.base_delay * (self.backoff_factor**attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            # Add random jitter between 0% and 25%
            jitter_amount = random.uniform(0, 0.25)
            delay = delay * (1 + jitter_amount)

        return delay

    def should_retry(self, exception: Exception) -> bool:
        """
        Determine if an exception should trigger a retry.

        Args:
            exception: The exception that was raised

        Returns:
            True if the exception should trigger a retry, False otherwise
        """
        if not self.retryable_exceptions:
            # If no specific exceptions are specified, retry on any exception
            return True

        return any(isinstance(exception, exc_type) for exc_type in self.retryable_exceptions)


def retry(
    max_attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 10.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Optional[List[Type[Exception]]] = None,
) -> Callable[[F], F]:
    """
    Decorator for retrying a function on failure.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Factor by which to increase delay after each attempt
        jitter: Whether to add random jitter to delay
        retryable_exceptions: List of exception types that should trigger a retry

    Returns:
        Decorated function
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        backoff_factor=backoff_factor,
        jitter=jitter,
        retryable_exceptions=retryable_exceptions,
    )

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if not config.should_retry(e) or attempt == config.max_attempts - 1:
                        raise

                    delay = config.calculate_delay(attempt)
                    logger.warning(f"Retry {attempt + 1}/{config.max_attempts} after {delay:.2f}s due to: {str(e)}")
                    time.sleep(delay)

            # This should never happen, but just in case
            if last_exception:
                raise last_exception
            return None  # For type checking

        return cast(F, wrapper)

    return decorator


def async_retry(
    max_attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 10.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Optional[List[Type[Exception]]] = None,
) -> Callable[[AsyncF], AsyncF]:
    """
    Decorator for retrying an async function on failure.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Factor by which to increase delay after each attempt
        jitter: Whether to add random jitter to delay
        retryable_exceptions: List of exception types that should trigger a retry

    Returns:
        Decorated async function
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        backoff_factor=backoff_factor,
        jitter=jitter,
        retryable_exceptions=retryable_exceptions,
    )

    def decorator(func: AsyncF) -> AsyncF:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for attempt in range(config.max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if not config.should_retry(e) or attempt == config.max_attempts - 1:
                        raise

                    delay = config.calculate_delay(attempt)
                    logger.warning(f"Async retry {attempt + 1}/{config.max_attempts} after {delay:.2f}s due to: {str(e)}")
                    await asyncio.sleep(delay)

            # This should never happen, but just in case
            if last_exception:
                raise last_exception
            return None  # For type checking

        return cast(AsyncF, wrapper)

    return decorator


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.

    The circuit breaker prevents repeated failures by temporarily disabling
    operations after a certain number of consecutive failures.

    States:
    - CLOSED: Normal operation, requests are allowed
    - OPEN: Circuit is broken, requests are not allowed
    - HALF_OPEN: Testing if the circuit can be closed again

    Attributes:
        failure_threshold: Number of consecutive failures before opening the circuit
        recovery_timeout: Time in seconds before attempting recovery
        reset_timeout: Time in seconds before resetting failure count
    """

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        reset_timeout: float = 60.0,
    ) -> None:
        """Initialize the circuit breaker."""
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.reset_timeout = reset_timeout
        self.state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.last_success_time = 0.0

    def record_success(self) -> None:
        """Record a successful operation."""
        self.failure_count = 0
        self.last_success_time = time.time()
        if self.state == self.HALF_OPEN:
            self.state = self.CLOSED
            logger.info("Circuit breaker closed after successful recovery")

    def record_failure(self) -> None:
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.state == self.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = self.OPEN
            logger.warning(f"Circuit breaker opened after {self.failure_count} consecutive failures")
        elif self.state == self.HALF_OPEN:
            self.state = self.OPEN
            logger.warning("Circuit breaker reopened after failed recovery attempt")

    def allow_request(self) -> bool:
        """
        Check if a request should be allowed.

        Returns:
            True if the request should be allowed, False otherwise
        """
        now = time.time()

        # Reset failure count if enough time has passed since the last failure
        if self.state == self.CLOSED and self.failure_count > 0 and now - self.last_failure_time > self.reset_timeout:
            self.failure_count = 0

        # Check if we should try recovery
        if self.state == self.OPEN and now - self.last_failure_time > self.recovery_timeout:
            self.state = self.HALF_OPEN
            logger.info("Circuit breaker half-open, attempting recovery")

        return self.state != self.OPEN

    def __call__(self, func: F) -> F:
        """
        Decorator for applying circuit breaker to a function.

        Args:
            func: Function to decorate

        Returns:
            Decorated function
        """

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not self.allow_request():
                raise CircuitBreakerOpenError(
                    f"Circuit breaker is open, request not allowed. "
                    f"Try again in {self.recovery_timeout - (time.time() - self.last_failure_time):.1f}s"
                )

            try:
                result = func(*args, **kwargs)
                self.record_success()
                return result
            except Exception:
                self.record_failure()
                raise

        return cast(F, wrapper)

    def async_call(self, func: AsyncF) -> AsyncF:
        """
        Decorator for applying circuit breaker to an async function.

        Args:
            func: Async function to decorate

        Returns:
            Decorated async function
        """

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not self.allow_request():
                raise CircuitBreakerOpenError(
                    f"Circuit breaker is open, request not allowed. "
                    f"Try again in {self.recovery_timeout - (time.time() - self.last_failure_time):.1f}s"
                )

            try:
                result = await func(*args, **kwargs)
                self.record_success()
                return result
            except Exception:
                self.record_failure()
                raise

        return cast(AsyncF, wrapper)


class CircuitBreakerOpenError(Exception):
    """Exception raised when a circuit breaker is open."""
