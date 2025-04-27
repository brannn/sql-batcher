"""
Insert statement merger for SQL Batcher.

This module provides functionality to merge INSERT statements for better performance.
"""

from typing import List, Optional, TypeVar


import re

T = TypeVar('T', bound=str)

class InsertMerger:
    """
    Merger for SQL INSERT statements.

    This class provides functionality to merge multiple INSERT statements into a
    single statement where possible. It handles various INSERT statement formats
    and ensures that only compatible statements are merged.
    """

    def __init__(self) -> None:
        """Initialize the insert merger."""
        self._table_pattern = re.compile(r"INSERT\s+INTO\s+(\w+)", re.IGNORECASE)
        self._columns_pattern = re.compile(
            r"INSERT\s+INTO\s+\w+\s*\(([^)]*)\)", re.IGNORECASE
        )
        self._values_pattern = re.compile(r"VALUES\s*\(([^)]*)\)", re.IGNORECASE)
        self.current_batch: List[T] = []

    def _extract_table_name(self, statement: str) -> Optional[str]:
        """
        Extract the table name from an INSERT statement.

        Args:
            statement: SQL INSERT statement

        Returns:
            Table name if found, None otherwise
        """
        match = self._table_pattern.search(statement)
        return match.group(1) if match else None

    def _extract_columns(self, statement: str) -> Optional[List[str]]:
        """
        Extract column names from an INSERT statement.

        Args:
            statement: SQL INSERT statement

        Returns:
            List of column names if found, None otherwise
        """
        match = self._columns_pattern.search(statement)
        if not match:
            return None
        return [col.strip() for col in match.group(1).split(",")]

    def _extract_values(self, statement: str) -> Optional[List[str]]:
        """
        Extract values from an INSERT statement.

        Args:
            statement: SQL INSERT statement

        Returns:
            List of values if found, None otherwise
        """
        match = self._values_pattern.search(statement)
        if not match:
            return None
        return [val.strip() for val in match.group(1).split(",")]

    def _are_compatible(self, stmt1: T, stmt2: T) -> bool:
        """
        Check if two INSERT statements are compatible for merging.

        Args:
            stmt1: First INSERT statement
            stmt2: Second INSERT statement

        Returns:
            True if statements can be merged, False otherwise
        """
        # Check if both statements target the same table
        table1 = self._extract_table_name(stmt1)
        table2 = self._extract_table_name(stmt2)
        if table1 != table2:
            return False

        # Check if both statements have the same columns
        cols1 = self._extract_columns(stmt1)
        cols2 = self._extract_columns(stmt2)
        if cols1 != cols2:
            return False

        return True

    def _merge_values(self, stmt1: T, stmt2: T) -> Optional[T]:
        """
        Merge values from two compatible INSERT statements.

        Args:
            stmt1: First INSERT statement
            stmt2: Second INSERT statement

        Returns:
            Merged INSERT statement if successful, None otherwise
        """
        if not self._are_compatible(stmt1, stmt2):
            return None

        # Extract values from both statements
        values1 = self._extract_values(stmt1)
        values2 = self._extract_values(stmt2)
        if not values1 or not values2:
            return None

        # Get the base statement (everything before VALUES)
        base_match = re.match(r"(.*?VALUES\s*\()", stmt1, re.IGNORECASE)
        if not base_match:
            return None
        base = base_match.group(1)

        # Combine values
        combined_values = f"{values1[0]}, {values2[0]}"
        for v1, v2 in zip(values1[1:], values2[1:]):
            combined_values += f", {v1}, {v2}"

        # Construct the merged statement
        return f"{base}{combined_values})"

    def merge(self, statements: List[T]) -> List[T]:
        """
        Merge compatible INSERT statements.

        Args:
            statements: List of SQL statements to merge

        Returns:
            List of merged SQL statements
        """
        if not statements:
            return []

        result: List[T] = []
        current: Optional[T] = None

        for stmt in statements:
            # Skip non-INSERT statements
            if not stmt.strip().upper().startswith("INSERT"):
                if current:
                    result.append(current)
                    current = None
                result.append(stmt)
                continue

            # Try to merge with current statement
            if current and self._are_compatible(current, stmt):
                merged = self._merge_values(current, stmt)
                if merged:
                    current = merged
                    continue

            # If we can't merge, add current to result and start new
            if current:
                result.append(current)
            current = stmt

        # Add the last statement if any
        if current:
            result.append(current)

        return result

    def flush_all(self) -> List[T]:
        """
        Flush all buffered statements and return them as a list of merged statements.

        Returns:
            List of merged SQL statements
        """
        # Get all statements from the current batch
        statements = self.current_batch.copy()
        
        # Clear the current batch
        self.current_batch = []
        
        # Merge the statements
        return self.merge(statements)
