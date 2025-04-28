"""
Insert statement merger for SQL Batcher.

This module provides functionality to merge INSERT statements for better performance.
"""

from typing import List, Optional, TypeVar, Dict

T = TypeVar("T", bound=str)


class InsertMerger:
    """
    Merger for SQL INSERT statements.

    This class provides functionality to merge multiple INSERT statements into a
    single statement where possible. It handles various INSERT statement formats
    and ensures that only compatible statements are merged.
    """

    def __init__(self, max_bytes: Optional[int] = None) -> None:
        """Initialize an InsertMerger.

        Args:
            max_bytes: Maximum size in bytes for merged statements
        """
        self.max_bytes = max_bytes or 900_000
        self.table_maps = {}

    def _extract_table_name(self, statement: str) -> Optional[str]:
        """Extract table name from INSERT statement."""
        parts = statement.split()
        try:
            return parts[parts.index("INTO") + 1].strip()
        except (ValueError, IndexError):
            return None

    def _extract_columns(self, statement: str) -> Optional[List[str]]:
        """Extract column definition from INSERT statement."""
        try:
            # Find the first opening parenthesis after INTO table_name
            table_end = statement.upper().index("INTO") + 4
            table_end = statement.index(" ", table_end)
            start = statement.index("(", table_end)
            
            # If we find VALUES before the opening parenthesis, there are no columns
            values_pos = statement.upper().index("VALUES")
            if values_pos < start:
                return None
            
            end = statement.index(")", start)
            columns_str = statement[start + 1:end]
            return [col.strip() for col in columns_str.split(",")]
        except ValueError:
            return None

    def _extract_values(self, statement: str) -> Optional[List[str]]:
        """Extract values from INSERT statement."""
        try:
            values_start = statement.upper().index("VALUES")
            start = statement.index("(", values_start)
            end = statement.index(")", start)
            values_str = statement[start + 1:end]
            return [val.strip() for val in values_str.split(",")]
        except ValueError:
            return None

    def _are_compatible(self, stmt1: str, stmt2: str) -> bool:
        """Check if two INSERT statements are compatible for merging."""
        table1 = self._extract_table_name(stmt1)
        table2 = self._extract_table_name(stmt2)
        if table1 != table2:
            return False

        cols1 = self._extract_columns(stmt1)
        cols2 = self._extract_columns(stmt2)
        if cols1 != cols2:
            return False

        return True

    def _merge_values(self, stmt1: str, stmt2: str) -> Optional[str]:
        """Merge values from two compatible INSERT statements."""
        if not self._are_compatible(stmt1, stmt2):
            return None

        table = self._extract_table_name(stmt1)
        columns = self._extract_columns(stmt1)
        values1 = self._extract_values(stmt1)
        values2 = self._extract_values(stmt2)

        if not all([table, values1, values2]):
            return None

        # Format the merged statement
        cols_part = f" ({', '.join(columns)})" if columns else ""
        values_str = f"({', '.join(values1)}), ({', '.join(values2)})"
        return f"INSERT INTO {table}{cols_part} VALUES {values_str}"

    def merge(self, statements: List[str]) -> List[str]:
        """Merge compatible INSERT statements.

        Args:
            statements: List of SQL statements to merge

        Returns:
            List of merged SQL statements
        """
        if not statements:
            return []

        result: List[str] = []
        non_inserts: List[str] = []
        table_groups: Dict[str, List[str]] = {}

        # Group statements by table
        for stmt in statements:
            if not stmt.strip().upper().startswith("INSERT"):
                non_inserts.append(stmt)
                continue

            table = self._extract_table_name(stmt)
            if not table:
                non_inserts.append(stmt)
                continue

            if table not in table_groups:
                table_groups[table] = []
            table_groups[table].append(stmt)

        # Process each table group
        for table_stmts in table_groups.values():
            if not table_stmts:
                continue

            # Try to merge all compatible statements
            current = table_stmts[0]
            compatible_group = [current]

            for stmt in table_stmts[1:]:
                if self._are_compatible(compatible_group[0], stmt):
                    compatible_group.append(stmt)
                else:
                    # Merge current group
                    if len(compatible_group) == 1:
                        result.append(compatible_group[0])
                    else:
                        table = self._extract_table_name(compatible_group[0])
                        columns = self._extract_columns(compatible_group[0])
                        all_values = []
                        for s in compatible_group:
                            values = self._extract_values(s)
                            if values:
                                all_values.append(f"({', '.join(values)})")
                        cols_part = f" ({', '.join(columns)})" if columns else ""
                        merged = f"INSERT INTO {table}{cols_part} VALUES {', '.join(all_values)}"
                        result.append(merged)
                    # Start new group
                    current = stmt
                    compatible_group = [current]

            # Merge final group
            if compatible_group:
                if len(compatible_group) == 1:
                    result.append(compatible_group[0])
                else:
                    table = self._extract_table_name(compatible_group[0])
                    columns = self._extract_columns(compatible_group[0])
                    all_values = []
                    for s in compatible_group:
                        values = self._extract_values(s)
                        if values:
                            all_values.append(f"({', '.join(values)})")
                    cols_part = f" ({', '.join(columns)})" if columns else ""
                    merged = f"INSERT INTO {table}{cols_part} VALUES {', '.join(all_values)}"
                    result.append(merged)

        # Interleave non-INSERT statements with merged statements
        if non_inserts:
            result = non_inserts + result

        return result

    def add_statement(self, statement: str) -> Optional[str]:
        """Add a statement to the merger.

        Args:
            statement: SQL statement to add

        Returns:
            Merged statement if flush occurred, None otherwise
        """
        if not statement.strip().upper().startswith("INSERT"):
            return statement

        table_name = self._extract_table_name(statement)
        if not table_name:
            return statement

        columns = self._extract_columns(statement)
        values = self._extract_values(statement)
        if not values:
            return statement

        # Format values for storage
        value_str = f"({', '.join(values)})"

        if table_name not in self.table_maps:
            self.table_maps[table_name] = {
                'columns': f"({', '.join(columns)})" if columns else None,
                'values': []
            }
        elif self.table_maps[table_name]['columns'] != (f"({', '.join(columns)})" if columns else None):
            return statement

        table_map = self.table_maps[table_name]

        # Check if we should flush based on size and number of values
        if self.max_bytes and table_map['values']:
            cols_part = f" ({', '.join(columns)})" if columns else ""
            test_values = table_map['values'] + [value_str]
            values_str = ", ".join(test_values)
            merged = f"INSERT INTO {table_name}{cols_part} VALUES {values_str}"
            merged_size = len(merged.encode())
            
            # Flush if we have 2 values and adding another would make us close to the limit
            if len(table_map['values']) >= 2 and merged_size > self.max_bytes * 0.6:
                values_str = ", ".join(table_map['values'])
                result = f"INSERT INTO {table_name}{cols_part} VALUES {values_str}"
                table_map['values'] = [value_str]
                return result

        # Add the value to the batch
        table_map['values'].append(value_str)
        return None

    def flush_table(self, table_name: str) -> Optional[str]:
        """Flush statements for a specific table.

        Args:
            table_name: Name of table to flush

        Returns:
            Merged SQL statement if any values exist
        """
        if table_name not in self.table_maps:
            return None

        table_map = self.table_maps[table_name]
        if not table_map['values']:
            return None

        cols_part = f" {table_map['columns']}" if table_map['columns'] else ""
        values_str = ", ".join(table_map['values'])
        merged = f"INSERT INTO {table_name}{cols_part} VALUES {values_str}"
        table_map['values'] = []
        return merged

    def flush_all(self) -> List[str]:
        """Flush all buffered statements.

        Returns:
            List of merged SQL statements
        """
        result = []
        for table_name in list(self.table_maps.keys()):
            merged = self.flush_table(table_name)
            if merged:
                result.append(merged)
        return result

    def get_merged_statements(self) -> List[str]:
        """Get all merged statements without flushing.

        Returns:
            List of merged SQL statements
        """
        result = []
        for table_name, table_map in self.table_maps.items():
            if not table_map['values']:
                continue

            cols_part = f" {table_map['columns']}" if table_map['columns'] else ""
            values_str = ", ".join(table_map['values'])
            merged = f"INSERT INTO {table_name}{cols_part} VALUES {values_str}"
            result.append(merged)
        return result
