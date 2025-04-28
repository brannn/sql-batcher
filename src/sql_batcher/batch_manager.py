"""
BatchManager: A tool for managing SQL statement batches.

This module provides functionality to manage batches of SQL statements
for efficient processing.
"""

from typing import List, Optional

class BatchManager:
    """Manages batches of SQL statements."""

    def __init__(self, max_bytes: int = 900_000) -> None:
        """Initialize the batch manager.

        Args:
            max_bytes: Maximum size in bytes for a batch
        """
        self.max_bytes = max_bytes
        self.current_batch: List[str] = []
        self.current_size = 0
        self.adjustment_factor = 1.0

    def add_statement(self, statement: str) -> bool:
        """Add a statement to the current batch.

        Args:
            statement: SQL statement to add

        Returns:
            True if the batch should be flushed, False otherwise
        """
        if not statement:
            return False

        # Check if adding this statement would exceed the limit
        statement_size = len(statement.encode())
        new_size = self.current_size + statement_size

        if new_size > self.max_bytes:
            return True

        # Add the statement to the current batch
        self.current_batch.append(statement)
        self.current_size = new_size
        return False

    def get_batch(self) -> List[str]:
        """Get the current batch of statements.

        Returns:
            List of statements in the current batch
        """
        return self.current_batch.copy()

    def reset(self) -> None:
        """Reset the batch manager state."""
        self.current_batch = []
        self.current_size = 0

    def adjust_for_columns(self, column_count: int, reference_count: int) -> None:
        """Adjust batch size based on column count.

        Args:
            column_count: Number of columns in the current statement
            reference_count: Reference number of columns
        """
        if column_count > reference_count:
            self.adjustment_factor = reference_count / column_count
        else:
            self.adjustment_factor = 1.0
