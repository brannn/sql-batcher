"""Additional tests for the retry mechanism to improve coverage."""

import unittest
from unittest.mock import MagicMock, patch

import asyncio

from sql_batcher.retry import RetryConfig, async_retry, retry


class TestRetryEdgeCases(unittest.TestCase):
    """Test edge cases for the retry mechanism."""

    @patch("time.sleep")
    def test_retry_with_min_max_attempts(self, mock_sleep):
        """Test retry with minimum max_attempts (1)."""
        # Create a mock function
        mock_func = MagicMock(return_value="success")

        # Apply the retry decorator with minimum max_attempts
        # This should still allow the function to be called once
        decorated_func = retry(max_attempts=1)(mock_func)

        # Call the decorated function
        result = decorated_func()

        # Verify the function was called once
        self.assertEqual(mock_func.call_count, 1)
        self.assertEqual(result, "success")
        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_retry_with_small_delays(self, mock_sleep):
        """Test retry with small delay values."""
        # Create a mock function that fails once and then succeeds
        mock_func = MagicMock(side_effect=[Exception("Error"), "success"])

        # Apply the retry decorator with small delay values
        decorated_func = retry(base_delay=0.001, max_delay=0.01)(mock_func)

        # Call the decorated function
        result = decorated_func()

        # Verify the function was called twice
        self.assertEqual(mock_func.call_count, 2)
        self.assertEqual(result, "success")
        # Sleep should be called with a small value
        mock_sleep.assert_called_once()
        self.assertGreaterEqual(mock_sleep.call_args[0][0], 0)

    def test_retry_config_with_empty_retryable_exceptions(self):
        """Test RetryConfig with empty retryable_exceptions."""
        config = RetryConfig(retryable_exceptions=[])

        # With empty retryable_exceptions, all exceptions should be retried
        self.assertTrue(config.should_retry(ValueError()))
        self.assertTrue(config.should_retry(KeyError()))
        self.assertTrue(config.should_retry(Exception()))

    def test_retry_config_with_none_retryable_exceptions(self):
        """Test RetryConfig with None retryable_exceptions."""
        config = RetryConfig(retryable_exceptions=None)

        # With None retryable_exceptions, all exceptions should be retried
        self.assertTrue(config.should_retry(ValueError()))
        self.assertTrue(config.should_retry(KeyError()))
        self.assertTrue(config.should_retry(Exception()))

    def test_retry_config_with_subclass_exceptions(self):
        """Test RetryConfig with exception subclasses."""

        # Create a custom exception hierarchy
        class CustomError(Exception):
            pass

        class SpecificError(CustomError):
            pass

        # Configure to retry on CustomError
        config = RetryConfig(retryable_exceptions=[CustomError])

        # SpecificError is a subclass of CustomError, so it should be retried
        self.assertTrue(config.should_retry(CustomError()))
        self.assertTrue(config.should_retry(SpecificError()))
        self.assertFalse(config.should_retry(ValueError()))

    @patch("time.sleep")
    def test_retry_preserves_function_metadata(self, mock_sleep):
        """Test that retry preserves function metadata."""

        # Define a function with docstring and metadata
        def test_func(a, b):
            """Test function docstring."""
            return a + b

        # Apply the retry decorator
        decorated_func = retry()(test_func)

        # Check that metadata is preserved
        self.assertEqual(decorated_func.__name__, "test_func")
        self.assertEqual(decorated_func.__doc__, "Test function docstring.")

        # Check that the function still works
        self.assertEqual(decorated_func(1, 2), 3)


class TestAsyncRetry(unittest.IsolatedAsyncioTestCase):
    """Test the async_retry decorator."""

    @patch("asyncio.sleep")
    async def test_async_retry_success_first_try(self, mock_sleep):
        """Test that an async function that succeeds on the first try is called only once."""

        # Create a mock async function that always succeeds
        async def mock_async_func():
            return "success"

        mock_func = MagicMock(side_effect=mock_async_func)

        # Apply the async_retry decorator
        decorated_func = async_retry()(mock_func)

        # Call the decorated function
        result = await decorated_func()

        # Verify the function was called only once
        self.assertEqual(mock_func.call_count, 1)
        self.assertEqual(result, "success")
        mock_sleep.assert_not_called()

    @patch("asyncio.sleep")
    async def test_async_retry_success_after_retries(self, mock_sleep):
        """Test that an async function that succeeds after retries is called the expected number of times."""

        # Create async functions for the side effects
        async def fail1():
            raise Exception("Error 1")

        async def fail2():
            raise Exception("Error 2")

        async def succeed():
            return "success"

        # Create a mock async function that fails twice and then succeeds
        mock_func = MagicMock(side_effect=[fail1(), fail2(), succeed()])

        # Apply the async_retry decorator
        decorated_func = async_retry(max_attempts=3, base_delay=0.01)(mock_func)

        # Call the decorated function
        result = await decorated_func()

        # Verify the function was called three times
        self.assertEqual(mock_func.call_count, 3)
        self.assertEqual(result, "success")
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called twice for retries

    @patch("asyncio.sleep")
    async def test_async_retry_max_attempts_exceeded(self, mock_sleep):
        """Test that an async function that always fails raises an exception after max attempts."""
        # Create a mock async function that always fails
        mock_func = MagicMock(side_effect=Exception("Error"))

        # Apply the async_retry decorator
        decorated_func = async_retry(max_attempts=3, base_delay=0.01)(mock_func)

        # Call the decorated function and verify it raises an exception
        with self.assertRaises(Exception):
            await decorated_func()

        # Verify the function was called the expected number of times
        self.assertEqual(mock_func.call_count, 3)  # Initial call + 2 retries
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called twice for retries

    @patch("asyncio.sleep")
    async def test_async_retry_with_specific_exceptions(self, mock_sleep):
        """Test that only specified exceptions trigger retries for async functions."""
        # Create a mock async function that raises different exceptions
        mock_func = MagicMock(side_effect=[ValueError("Error"), KeyError("Error")])

        # Apply the async_retry decorator, only retrying on ValueError
        decorated_func = async_retry(retryable_exceptions=[ValueError])(mock_func)

        # Call the decorated function and verify it raises KeyError
        with self.assertRaises(KeyError):
            await decorated_func()

        # Verify the function was called twice (initial call with ValueError, then KeyError)
        self.assertEqual(mock_func.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)  # Sleep called once for retry

    @patch("asyncio.sleep")
    async def test_async_retry_with_args_and_kwargs(self, mock_sleep):
        """Test that args and kwargs are passed correctly to the async function."""

        # Create a mock async function that returns its args and kwargs
        async def test_func(*args, **kwargs):
            return args, kwargs

        # Apply the async_retry decorator
        decorated_func = async_retry()(test_func)

        # Call the decorated function with args and kwargs
        result = await decorated_func(1, 2, 3, a=1, b=2)

        # Verify the args and kwargs were passed correctly
        self.assertEqual(result, ((1, 2, 3), {"a": 1, "b": 2}))

    @patch("asyncio.sleep")
    async def test_async_retry_preserves_function_metadata(self, mock_sleep):
        """Test that async_retry preserves function metadata."""

        # Define an async function with docstring and metadata
        async def test_func(a, b):
            """Test async function docstring."""
            return a + b

        # Apply the async_retry decorator
        decorated_func = async_retry()(test_func)

        # Check that metadata is preserved
        self.assertEqual(decorated_func.__name__, "test_func")
        self.assertEqual(decorated_func.__doc__, "Test async function docstring.")

        # Check that the function still works
        self.assertEqual(await decorated_func(1, 2), 3)

    @patch("asyncio.sleep")
    async def test_async_retry_with_real_async_function(self, mock_sleep):
        """Test async_retry with a real async function that uses await."""

        # Define an async function that uses await
        async def async_operation():
            # Simulate an async operation
            await asyncio.sleep(0.001)
            return "async result"

        # Create a function that fails first then succeeds
        call_count = 0

        async def side_effect_func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Error")
            return await async_operation()

        # Apply the async_retry decorator
        decorated_func = async_retry(max_attempts=3, base_delay=0.01)(side_effect_func)

        # Call the decorated function
        result = await decorated_func()

        # Verify the result
        self.assertEqual(result, "async result")
        self.assertEqual(call_count, 2)  # Function called twice
        # The sleep is called twice: once for the retry and once inside async_operation
        self.assertEqual(mock_sleep.call_count, 2)
