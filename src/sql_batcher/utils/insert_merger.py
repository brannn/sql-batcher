"""
InsertMerger: A tool for merging compatible INSERT statements.

This module provides functionality to merge compatible INSERT statements
to reduce the number of database calls and improve performance.
"""

from typing import List, Optional, Dict, Any, Set, Tuple, Union
import re

from sql_batcher.exceptions import InsertMergerError

class InsertMerger:
    """Merges multiple SQL INSERT statements into a single statement."""

    def __init__(self, max_bytes: int = 900000) -> None:
        """Initialize the merger.

        Args:
            max_bytes: Maximum size in bytes for merged statements.
        """
        self.max_bytes = max_bytes
        self.table_maps = {}  # Maps table names to their current batch info

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL statement for consistent comparison.

        Args:
            sql: SQL statement to normalize.

        Returns:
            Normalized SQL statement.
        """
        # Preserve quoted strings while normalizing whitespace
        normalized = []
        in_quotes = False
        quote_char = None
        current_token = []

        for char in sql:
            if char in ["'", '"', '`']:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                current_token.append(char)
            elif in_quotes:
                current_token.append(char)
            elif char.isspace():
                if current_token:
                    normalized.append(''.join(current_token))
                    current_token = []
            else:
                current_token.append(char.upper())

        if current_token:
            normalized.append(''.join(current_token))

        return ' '.join(normalized)

    def _validate_sql(self, sql: str) -> None:
        """Validate SQL statement.

        Args:
            sql: SQL statement to validate.

        Raises:
            InsertMergerError: If SQL is invalid.
        """
        if not sql or not isinstance(sql, str):
            raise InsertMergerError("SQL statement must be a non-empty string")

        normalized = self._normalize_sql(sql.strip())
        if not normalized.upper().startswith("INSERT INTO"):
            raise InsertMergerError("Only INSERT statements are supported")

        # Check for balanced parentheses
        open_count = sql.count('(')
        close_count = sql.count(')')
        if open_count != close_count:
            raise InsertMergerError("Unbalanced parentheses in SQL statement")

        # Check for VALUES clause
        if "VALUES" not in normalized.upper():
            raise InsertMergerError("INSERT statement must contain VALUES clause")

        # Validate column names
        columns = self._extract_columns(sql)
        if columns:
            col_list = [c.strip() for c in columns.strip('()').split(',')]
            if not col_list:
                raise InsertMergerError("Empty column list")
            
            # Check for invalid column names
            for col in col_list:
                if not col.strip('`"\'').replace('_', '').isalnum():
                    raise InsertMergerError(f"Invalid column name: {col}")

            # Check for duplicate columns
            seen = set()
            for col in col_list:
                normalized_col = col.strip('`"\'').upper()
                if normalized_col in seen:
                    raise InsertMergerError(f"Duplicate column: {col}")
                seen.add(normalized_col)

    def _extract_table_name(self, sql: str) -> str:
        """Extract table name from SQL statement.

        Args:
            sql: SQL statement to extract from.

        Returns:
            Table name.

        Raises:
            InsertMergerError: If table name cannot be extracted.
        """
        match = re.search(r"INSERT\s+INTO\s+([`\"']?[\w.]+[`\"']?)", sql, re.IGNORECASE)
        if not match:
            raise InsertMergerError("Could not extract table name")
        return match.group(1).strip('`"\'')

    def _extract_columns(self, sql: str) -> Optional[str]:
        """Extract column list from SQL statement.

        Args:
            sql: SQL statement to extract from.

        Returns:
            Column list or None if implicit.
        """
        match = re.search(r"INSERT\s+INTO\s+[`\"']?[\w.]+[`\"']?\s*(\([^)]+\))?", sql, re.IGNORECASE)
        if match and match.group(1):
            return match.group(1)
        return None

    def _extract_values(self, sql: str) -> str:
        """Extract the VALUES clause from an SQL INSERT statement.
        
        Args:
            sql: SQL INSERT statement.
            
        Returns:
            The VALUES clause without the VALUES keyword.
            
        Raises:
            InsertMergerError: If VALUES clause cannot be extracted.
        """
        # Find VALUES keyword (case-insensitive)
        match = re.search(r'\bVALUES\s*\(', sql, re.IGNORECASE)
        if not match:
            raise InsertMergerError("No VALUES clause found in SQL statement")
        
        # Extract everything after VALUES
        values_start = match.start()
        values_str = sql[values_start:].strip()
        
        # Remove VALUES keyword
        values_str = re.sub(r'\bVALUES\s*', '', values_str, flags=re.IGNORECASE)
        
        # Validate balanced parentheses and quotes
        paren_count = 0
        in_quote = False
        quote_char = None
        in_json = False
        json_level = 0
        
        for char in values_str:
            if char in ["'", '"'] and (not quote_char or char == quote_char):
                in_quote = not in_quote
                quote_char = char if in_quote else None
            elif not in_quote:
                if char == '{':
                    in_json = True
                    json_level += 1
                elif char == '}':
                    json_level -= 1
                    if json_level == 0:
                        in_json = False
                elif not in_json:
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                        if paren_count < 0:
                            raise InsertMergerError("Unbalanced parentheses in VALUES clause")
        
        if paren_count != 0:
            raise InsertMergerError("Unbalanced parentheses in VALUES clause")
        
        return values_str

    def _are_compatible(self, stmt1: str, stmt2: str) -> bool:
        """Check if two statements are compatible for merging.

        Args:
            stmt1: First SQL statement.
            stmt2: Second SQL statement.

        Returns:
            True if statements are compatible.
        """
        # Check table names (case-insensitive)
        if self._extract_table_name(stmt1).lower() != self._extract_table_name(stmt2).lower():
            return False

        cols1 = self._extract_columns(stmt1)
        cols2 = self._extract_columns(stmt2)

        # If either statement has implicit columns, they're not compatible
        if not cols1 or not cols2:
            return cols1 == cols2

        # Normalize column names for case-insensitive comparison
        cols1_norm = [c.strip('`"\'').lower() for c in cols1.strip('()').split(',')]
        cols2_norm = [c.strip('`"\'').lower() for c in cols2.strip('()').split(',')]

        # Check if columns match (order doesn't matter)
        return set(cols1_norm) == set(cols2_norm)

    def merge(self, statements: List[str]) -> str:
        """Merge multiple SQL INSERT statements.

        Args:
            statements: List of SQL statements to merge.

        Returns:
            Merged SQL statement.

        Raises:
            InsertMergerError: If statements cannot be merged.
        """
        if not statements:
            raise InsertMergerError("No statements to merge")

        # Validate all statements
        for stmt in statements:
            self._validate_sql(stmt)

        # Check compatibility
        base_stmt = statements[0]
        for stmt in statements[1:]:
            if not self._are_compatible(base_stmt, stmt):
                raise InsertMergerError("Statements are not compatible for merging")

        # Extract components from first statement
        table = self._extract_table_name(base_stmt)
        columns = self._extract_columns(base_stmt)
        values = [self._extract_values(stmt) for stmt in statements]

        # Construct merged statement
        result = f"INSERT INTO {table}"
        if columns:
            result += f" {columns}"
        result += f" VALUES {', '.join(values)}"

        # Check size limit
        if len(result) > self.max_bytes:
            # If single statement exceeds limit, return as is
            if len(statements) == 1:
                return statements[0]
            # Otherwise, split into smaller batches
            mid = len(statements) // 2
            return self.merge(statements[:mid]) + "; " + self.merge(statements[mid:])

        return result

    def add_statement(self, sql: str) -> Optional[str]:
        """Add a statement to the merger.

        Args:
            sql: SQL statement to add.

        Returns:
            Flushed statements if batch is full, None otherwise.
        """
        self._validate_sql(sql)
        table = self._extract_table_name(sql)

        if table not in self.table_maps:
            self.table_maps[table] = {
                'statements': [],
                'columns': self._extract_columns(sql)
            }

        batch = self.table_maps[table]
        
        # Check compatibility
        if batch['statements'] and not self._are_compatible(batch['statements'][0], sql):
            flushed = self.flush_table(table)
            batch['statements'] = [sql]
            batch['columns'] = self._extract_columns(sql)
            return flushed

        batch['statements'].append(sql)
        
        # Check size limit
        current_size = sum(len(s) for s in batch['statements'])
        if current_size > self.max_bytes:
            return self.flush_table(table)

        return None

    def flush_table(self, table: str) -> str:
        """Flush statements for a specific table.

        Args:
            table: Table name to flush.

        Returns:
            Merged SQL statement.
        """
        if table not in self.table_maps or not self.table_maps[table]['statements']:
            return ""

        batch = self.table_maps[table]
        result = self.merge(batch['statements'])
        batch['statements'] = []
        return result

    def flush_all(self) -> List[str]:
        """Flush all buffered statements.

        Returns:
            List of merged SQL statements.
        """
        results = []
        for table in list(self.table_maps.keys()):
            flushed = self.flush_table(table)
            if flushed:
                results.append(flushed)
        return results

    def get_merged_statements(self) -> List[str]:
        """Get all merged statements.

        Returns:
            List of merged SQL statements.
        """
        return self.flush_all()

    def _merge_values(self, statements: List[str]) -> str:
        """Merge VALUES clauses from multiple SQL INSERT statements.
        
        Args:
            statements: List of SQL INSERT statements to merge.
            
        Returns:
            Merged VALUES clause.
        """
        if not statements:
            return ""
        
        # Extract values from each statement
        all_values = []
        for stmt in statements:
            values = self._extract_values(stmt)
            # Split on top-level commas (not inside parentheses or quotes)
            paren_level = 0
            in_quote = False
            quote_char = None
            last_split = 0
            value_parts = []
            
            for i, char in enumerate(values):
                if char in ["'", '"'] and (not quote_char or char == quote_char):
                    in_quote = not in_quote
                    quote_char = char if in_quote else None
                elif not in_quote:
                    if char == '(':
                        paren_level += 1
                    elif char == ')':
                        paren_level -= 1
                    elif char == ',' and paren_level == 0:
                        value_parts.append(values[last_split:i].strip())
                        last_split = i + 1
            
            if last_split < len(values):
                value_parts.append(values[last_split:].strip())
                
            all_values.extend(value_parts)
        
        # Join values with commas
        return ', '.join(v for v in all_values if v)
