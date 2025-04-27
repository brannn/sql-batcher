"""Tests for the RetryManager class."""

import asyncio
import pytest
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock

from sql_batcher.retry_manager import RetryManager
from sql_batcher.exceptions import RetryError


class TestRetryManager:
    """Test suite for RetryManager."""

    @pytest.fixture
    def retry_manager(self) -> RetryManager:
        """Create a RetryManager instance for testing."""
        return RetryManager(max_retries=2, retry_delay=0.1)

    @pytest.fixture
    def mock_operation(self) -> AsyncMock:
        """Create a mock async operation."""
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_successful_operation(
        self, retry_manager: RetryManager, mock_operation: AsyncMock
    ) -> None:
        """Test successful operation execution."""
        mock_operation.return_value = "success"
        result = await retry_manager.execute_with_retry(mock_operation)
        assert result == "success"
        assert mock_operation.call_count == 1

    @pytest.mark.asyncio
    async def test_retry_on_failure(
        self, retry_manager: RetryManager, mock_operation: AsyncMock
    ) -> None:
        """Test retry on operation failure."""
        mock_operation.side_effect = [
            Exception("First attempt"),
            Exception("Second attempt"),
            "success"
        ]
        result = await retry_manager.execute_with_retry(mock_operation)
        assert result == "success"
        assert mock_operation.call_count == 3

    @pytest.mark.asyncio
    async def test_max_retries_exceeded(
        self, retry_manager: RetryManager, mock_operation: AsyncMock
    ) -> None:
        """Test behavior when max retries are exceeded."""
        mock_operation.side_effect = Exception("Always fails")
        with pytest.raises(RetryError) as exc_info:
            await retry_manager.execute_with_retry(mock_operation)
        assert "Operation failed after 3 attempts" in str(exc_info.value)
        assert mock_operation.call_count == 3

    @pytest.mark.asyncio
    async def test_timeout(self, retry_manager: RetryManager) -> None:
        """Test operation timeout."""
        async def slow_operation() -> str:
            await asyncio.sleep(1)
            return "success"

        with pytest.raises(asyncio.TimeoutError):
            await retry_manager.execute_with_retry(
                slow_operation,
                timeout=0.1
            )

    @pytest.mark.asyncio
    async def test_retry_delay(
        self, retry_manager: RetryManager, mock_operation: AsyncMock
    ) -> None:
        """Test retry delay between attempts."""
        mock_operation.side_effect = [
            Exception("First attempt"),
            "success"
        ]
        start_time = asyncio.get_event_loop().time()
        await retry_manager.execute_with_retry(mock_operation)
        end_time = asyncio.get_event_loop().time()
        assert end_time - start_time >= 0.1  # Should wait at least retry_delay

    def test_retry_info(self, retry_manager: RetryManager) -> None:
        """Test retry configuration information."""
        info = retry_manager.get_retry_info()
        assert info["max_retries"] == 2
        assert info["retry_delay"] == 0.1
        assert info["timeout"] is None

    @pytest.mark.asyncio
    async def test_custom_logger(self) -> None:
        """Test custom logger usage."""
        mock_logger = MagicMock()
        manager = RetryManager(logger=mock_logger)
        mock_operation = AsyncMock(side_effect=Exception("Test error"))

        with pytest.raises(RetryError):
            await manager.execute_with_retry(mock_operation)

        assert mock_logger.warning.call_count > 0

    @pytest.mark.asyncio
    async def test_operation_with_args(
        self, retry_manager: RetryManager, mock_operation: AsyncMock
    ) -> None:
        """Test operation execution with arguments."""
        mock_operation.return_value = "success"
        result = await retry_manager.execute_with_retry(
            mock_operation,
            "arg1",
            "arg2",
            kwarg1="value1",
            kwarg2="value2"
        )
        assert result == "success"
        mock_operation.assert_called_once_with(
            "arg1",
            "arg2",
            kwarg1="value1",
            kwarg2="value2"
        )

    @pytest.mark.asyncio
    async def test_different_error_types(
        self, retry_manager: RetryManager, mock_operation: AsyncMock
    ) -> None:
        """Test handling of different error types."""
        mock_operation.side_effect = [
            ValueError("First error"),
            TypeError("Second error"),
            "success"
        ]
        result = await retry_manager.execute_with_retry(mock_operation)
        assert result == "success"
        assert mock_operation.call_count == 3 