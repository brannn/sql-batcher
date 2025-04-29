"""Tests for the retry mechanism."""

import unittest
from unittest.mock import MagicMock, patch

from sql_batcher.retry import RetryConfig, retry


class TestRetryConfig(unittest.TestCase):
    """Test the RetryConfig class."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        config = RetryConfig()
        self.assertEqual(config.max_attempts, 3)
        self.assertEqual(config.base_delay, 0.1)
        self.assertEqual(config.max_delay, 10.0)
        self.assertEqual(config.backoff_factor, 2.0)
        self.assertTrue(config.jitter)
        self.assertEqual(config.retryable_exceptions, [])

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=0.2,
            max_delay=5.0,
            backoff_factor=3.0,
            jitter=False,
            retryable_exceptions=[ValueError, KeyError],
        )
        self.assertEqual(config.max_attempts, 5)
        self.assertEqual(config.base_delay, 0.2)
        self.assertEqual(config.max_delay, 5.0)
        self.assertEqual(config.backoff_factor, 3.0)
        self.assertFalse(config.jitter)
        self.assertEqual(config.retryable_exceptions, [ValueError, KeyError])

    def test_calculate_delay_without_jitter(self):
        """Test delay calculation without jitter."""
        config = RetryConfig(base_delay=0.1, backoff_factor=2.0, max_delay=10.0, jitter=False)
        self.assertEqual(config.calculate_delay(0), 0.1)  # First attempt
        self.assertEqual(config.calculate_delay(1), 0.2)  # Second attempt
        self.assertEqual(config.calculate_delay(2), 0.4)  # Third attempt
        self.assertEqual(config.calculate_delay(3), 0.8)  # Fourth attempt

    def test_calculate_delay_with_max_delay(self):
        """Test delay calculation with max_delay."""
        config = RetryConfig(base_delay=0.1, backoff_factor=10.0, max_delay=1.0, jitter=False)
        self.assertEqual(config.calculate_delay(0), 0.1)  # First attempt
        self.assertEqual(config.calculate_delay(1), 1.0)  # Second attempt (capped at max_delay)
        self.assertEqual(config.calculate_delay(2), 1.0)  # Third attempt (capped at max_delay)

    @patch("random.uniform", return_value=0.1)
    def test_calculate_delay_with_jitter(self, mock_uniform):
        """Test delay calculation with jitter."""
        config = RetryConfig(base_delay=0.1, backoff_factor=2.0, max_delay=10.0, jitter=True)
        # With jitter of 0.1, the delay should be 0.1 * (1 + 0.1) = 0.11
        self.assertAlmostEqual(config.calculate_delay(0), 0.11)

    def test_should_retry_with_no_exceptions(self):
        """Test should_retry with no specific exceptions."""
        config = RetryConfig(retryable_exceptions=[])
        self.assertTrue(config.should_retry(ValueError()))
        self.assertTrue(config.should_retry(KeyError()))
        self.assertTrue(config.should_retry(Exception()))

    def test_should_retry_with_specific_exceptions(self):
        """Test should_retry with specific exceptions."""
        config = RetryConfig(retryable_exceptions=[ValueError, KeyError])
        self.assertTrue(config.should_retry(ValueError()))
        self.assertTrue(config.should_retry(KeyError()))
        self.assertFalse(config.should_retry(TypeError()))


class TestRetryDecorator(unittest.TestCase):
    """Test the retry decorator."""

    @patch("time.sleep")
    def test_retry_success_first_try(self, mock_sleep):
        """Test that a function that succeeds on the first try is called only once."""
        # Create a mock function that always succeeds
        mock_func = MagicMock(return_value="success")

        # Apply the retry decorator
        decorated_func = retry()(mock_func)

        # Call the decorated function
        result = decorated_func()

        # Verify the function was called only once
        self.assertEqual(mock_func.call_count, 1)
        self.assertEqual(result, "success")
        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_retry_success_after_retries(self, mock_sleep):
        """Test that a function that succeeds after retries is called the expected number of times."""
        # Create a mock function that fails twice and then succeeds
        mock_func = MagicMock(side_effect=[Exception("Error"), Exception("Error"), "success"])

        # Apply the retry decorator
        decorated_func = retry(max_attempts=3, base_delay=0.01)(mock_func)

        # Call the decorated function
        result = decorated_func()

        # Verify the function was called three times
        self.assertEqual(mock_func.call_count, 3)
        self.assertEqual(result, "success")
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called twice for retries

    @patch("time.sleep")
    def test_retry_max_attempts_exceeded(self, mock_sleep):
        """Test that a function that always fails raises an exception after max attempts."""
        # Create a mock function that always fails
        mock_func = MagicMock(side_effect=Exception("Error"))

        # Apply the retry decorator
        decorated_func = retry(max_attempts=3, base_delay=0.01)(mock_func)

        # Call the decorated function and verify it raises an exception
        with self.assertRaises(Exception):
            decorated_func()

        # Verify the function was called the expected number of times
        self.assertEqual(mock_func.call_count, 3)  # Initial call + 2 retries
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called twice for retries

    @patch("time.sleep")
    def test_retry_with_specific_exceptions(self, mock_sleep):
        """Test that only specified exceptions trigger retries."""
        # Create a mock function that raises different exceptions
        mock_func = MagicMock(side_effect=[ValueError("Error"), KeyError("Error")])

        # Apply the retry decorator, only retrying on ValueError
        decorated_func = retry(retryable_exceptions=[ValueError])(mock_func)

        # Call the decorated function and verify it raises KeyError
        with self.assertRaises(KeyError):
            decorated_func()

        # Verify the function was called twice (initial call with ValueError, then KeyError)
        self.assertEqual(mock_func.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)  # Sleep called once for retry

    @patch("time.sleep")
    def test_retry_with_args_and_kwargs(self, mock_sleep):
        """Test that args and kwargs are passed correctly to the function."""

        # Create a mock function that returns its args and kwargs
        def test_func(*args, **kwargs):
            return args, kwargs

        # Apply the retry decorator
        decorated_func = retry()(test_func)

        # Call the decorated function with args and kwargs
        result = decorated_func(1, 2, 3, a=1, b=2)

        # Verify the args and kwargs were passed correctly
        self.assertEqual(result, ((1, 2, 3), {"a": 1, "b": 2}))

    @patch("time.sleep")
    def test_async_retry(self, mock_sleep):
        """Test the async retry decorator."""
        # This is a basic test to ensure the async retry decorator exists
        # Full async testing would require asyncio test fixtures
        from sql_batcher.retry import async_retry

        # Verify the function exists
        self.assertTrue(callable(async_retry))
