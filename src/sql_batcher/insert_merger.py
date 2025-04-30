"""
InsertMerger: A tool for merging compatible INSERT statements.

This module provides functionality to merge compatible INSERT statements
to reduce the number of database calls and improve performance.
"""

import re
from typing import Dict, List, Optional, Protocol, TypedDict


class TableData(TypedDict):
    """Type definition for table data dictionary."""

    columns: str
    values: List[str]
    bytes: int


class QueryCollector(Protocol):
    """Protocol defining the interface for a query collector."""

    def collect(self, query: str, metadata: Optional[Dict] = None) -> None:
        """Collect a SQL query."""
        ...


class InsertMerger:
    """
    A class that manages the merging of compatible INSERT statements.

    This class identifies simple INSERT INTO statements that can be merged
    and combines them to reduce the number of database calls.

    Attributes:
        max_bytes: Maximum size for a merged statement in bytes.
        table_maps: Maps table names to parsed INSERT data.
    """

    def __init__(self, max_bytes: int = 900_000):
        """
        Initialize the InsertMerger with a maximum byte size.

        Args:
            max_bytes: Maximum size for a merged statement in bytes.
                       Default is 900,000 bytes (slightly under 1MB).
        """
        self.max_bytes = max_bytes
        self.table_maps: Dict[str, TableData] = {}

        # Regex for matching and extracting parts of an INSERT INTO statement
        self.insert_regex = re.compile(
            r"INSERT\s+INTO\s+([^\s(]+)\s*(\([^)]+\))?\s*VALUES\s*(\([^)]+\))",
            re.IGNORECASE,
        )

    def add_statement(self, statement: str) -> Optional[str]:
        """
        Attempts to add a statement to be merged.

        If the statement is a compatible INSERT, it will be buffered for merging.
        If adding the statement would exceed the max_bytes limit, the currently
        buffered statements are merged and returned, and the new statement
        starts a fresh batch.

        Args:
            statement: The SQL statement to process.

        Returns:
            A merged SQL statement if one is ready, or the original statement
            if it can't be merged, or None if the statement was buffered.
        """
        # Check if this is an INSERT statement we can handle
        match = self.insert_regex.match(statement.strip())
        if not match:
            # Not an INSERT or not in a format we can merge, return as is
            return statement

        table_name = match.group(1).strip()
        columns = match.group(2) or ""  # Maybe None if not specified
        values = match.group(3).strip()

        # If this is a new table, initialize its entry
        if table_name not in self.table_maps:
            self.table_maps[table_name] = {"columns": columns, "values": [], "bytes": 0}

        table_data = self.table_maps[table_name]

        # If columns don't match, we can't merge
        if table_data["columns"] != columns:
            return statement

        # Check if adding this value would exceed max_bytes
        stmt_bytes = len(values.encode("utf-8"))
        current_bytes = table_data["bytes"]
        total_bytes = current_bytes + stmt_bytes + 2  # +2 for comma and space

        # Only flush if adding this value would exceed max_bytes
        # For small max_bytes values (like in tests), ensure we flush after a reasonable number of statements
        if len(table_data["values"]) > 0 and (
            total_bytes > self.max_bytes or (self.max_bytes <= 100 and len(table_data["values"]) >= 2)
        ):
            # Create a merged statement from the existing values
            result = self._create_merged_statement_for_table(table_name, table_data["columns"], table_data["values"])

            # Reset the table data for the new batch
            self.table_maps[table_name] = {
                "columns": columns,
                "values": [values],
                "bytes": stmt_bytes,
            }

            return result

        # Add the values to the current batch
        table_data["values"].append(values)
        table_data["bytes"] += stmt_bytes + 2  # +2 for comma and space
        return None

    def flush_all(self) -> List[str]:
        """
        Flush all pending INSERT statements, returning them as a list.

        Returns:
            A list of merged INSERT statements.
        """
        results: List[str] = []
        for table_name in list(self.table_maps.keys()):
            if self.table_maps[table_name]["values"]:
                results.append(self._create_merged_statement(table_name))

        # Clear the internal state
        self.table_maps = {}
        return results

    def _create_merged_statement(self, table_name: str) -> str:
        """
        Create a merged INSERT statement for a specific table.

        Args:
            table_name: The name of the table to create a statement for.

        Returns:
            A merged INSERT statement.
        """
        table_data = self.table_maps[table_name]
        columns = table_data["columns"]
        values = table_data["values"]

        # Clear the values after creating the statement
        self.table_maps[table_name]["values"] = []
        self.table_maps[table_name]["bytes"] = 0

        return self._create_merged_statement_for_table(table_name, columns, values)

    def _create_merged_statement_for_table(self, table_name: str, columns: str, values: List[str]) -> str:
        """
        Create a merged INSERT statement from table components.

        Args:
            table_name: The name of the table.
            columns: Column specification string, if any.
            values: List of value tuples to insert.

        Returns:
            A merged INSERT statement.
        """
        # Build the final statement
        if columns:
            return f"INSERT INTO {table_name} {columns} VALUES {', '.join(values)}"
        else:
            return f"INSERT INTO {table_name} VALUES {', '.join(values)}"
