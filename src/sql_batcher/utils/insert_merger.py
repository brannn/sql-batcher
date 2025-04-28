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

        # Check for balanced parentheses and quotes
        paren_stack = []
        in_quote = None
        in_json = False
        json_level = 0
        
        for i, char in enumerate(sql):
            # Handle quotes
            if char in '`"\'' and (not in_quote or char == in_quote):
                in_quote = None if in_quote else char
                continue
                
            if not in_quote:
                if char == '{':
                    in_json = True
                    json_level += 1
                elif char == '}':
                    json_level -= 1
                    if json_level == 0:
                        in_json = False
                elif not in_json:
                    if char == '(':
                        paren_stack.append(i)
                    elif char == ')':
                        if not paren_stack:
                            raise InsertMergerError("Unbalanced parentheses in SQL statement")
                        paren_stack.pop()

        if paren_stack:
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
            seen = set()
            for col in col_list:
                col_norm = col.strip('`"\'')
                # Allow any non-empty column name that doesn't contain whitespace
                if not col_norm or ' ' in col_norm:
                    raise InsertMergerError(f"Invalid column name: {col}")
                if col_norm.lower() in seen:
                    raise InsertMergerError(f"Duplicate column: {col}")
                seen.add(col_norm.lower())

    def _extract_table_name(self, sql: Union[str, List[str]]) -> str:
        """Extract table name from SQL statement.

        Args:
            sql: SQL statement to extract from. Can be a single string or list of strings.

        Returns:
            Table name.

        Raises:
            InsertMergerError: If table name cannot be extracted.
        """
        if isinstance(sql, list):
            if not sql:
                raise InsertMergerError("Empty list of statements")
            sql = sql[0]  # Use first statement for table name

        # Match table name with optional quotes and schema
        match = re.search(r"INSERT\s+INTO\s+(?:([`\"']?)([^`\"'\.\s]+)\1\.)?([`\"']?)([^`\"'\.\s]+)\3", sql, re.IGNORECASE)
        if not match:
            raise InsertMergerError("Could not extract table name")
        
        # Extract schema and table parts
        schema_quote, schema, table_quote, table = match.groups()
        
        # If schema is present, include it in the result
        if schema:
            return f"{schema_quote}{schema}{schema_quote}.{table_quote}{table}{table_quote}"
        return f"{table_quote}{table}{table_quote}"

    def _extract_columns(self, sql: Union[str, List[str]]) -> Optional[str]:
        """Extract column list from SQL statement.

        Args:
            sql: SQL statement to extract from. Can be a single string or list of strings.

        Returns:
            Column list with parentheses if present, None otherwise.
        """
        if isinstance(sql, list):
            if not sql:
                return None
            sql = sql[0]  # Use first statement for columns

        # Find opening parenthesis after INSERT INTO table
        table_end = re.search(r"INSERT\s+INTO\s+(?:[`\"']?[\w.]+[`\"']?\s*\.?\s*)*[`\"']?[\w.]+[`\"']?\s*", sql, re.IGNORECASE)
        if not table_end:
            return None
        
        pos = table_end.end()
        if pos >= len(sql) or sql[pos] != '(':
            return None

        # Track parentheses and quotes
        paren_count = 0
        in_quote = None
        start_pos = pos
        
        for i in range(pos, len(sql)):
            char = sql[i]
            
            # Handle quotes
            if char in '`"\'' and (not in_quote or char == in_quote):
                in_quote = None if in_quote else char
                continue
                
            if not in_quote:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        # Found matching closing parenthesis
                        return sql[start_pos:i+1].strip()
        
        return None

    def _extract_values(self, sql: Union[str, List[str]]) -> Optional[str]:
        """Extract values from SQL statement.

        Args:
            sql: SQL statement to extract from. Can be a single string or list of strings.

        Returns:
            Values clause with parentheses if present, None otherwise.
        """
        if isinstance(sql, list):
            if not sql:
                return None
            sql = sql[0]  # Use first statement for values

        # Find VALUES keyword
        values_match = re.search(r"\bVALUES\s*", sql, re.IGNORECASE)
        if not values_match:
            return None
            
        pos = values_match.end()
        if pos >= len(sql):
            return None
            
        # Track parentheses and quotes
        paren_count = 0
        in_quote = None
        start_pos = pos
        found_values = False
        
        for i in range(pos, len(sql)):
            char = sql[i]
            
            # Handle quotes
            if char in '`"\'' and (not in_quote or char == in_quote):
                in_quote = None if in_quote else char
                continue
                
            if not in_quote:
                if char == '(':
                    paren_count += 1
                    found_values = True
                elif char == ')':
                    paren_count -= 1
                    
                # Stop at end of values or next SQL keyword
                if (paren_count == 0 and found_values) or \
                   (char.isalpha() and sql[i:].upper().startswith(('ON ', 'WHERE ', 'RETURNING '))):
                    return sql[start_pos:i + (1 if paren_count == 0 else 0)].strip()
        
        return None if paren_count != 0 else sql[start_pos:].strip()

    def _are_compatible(self, stmt1: str, stmt2: str) -> bool:
        """Check if two statements are compatible for merging.

        Args:
            stmt1: First SQL statement.
            stmt2: Second SQL statement.

        Returns:
            True if statements are compatible.
        """
        # Extract table names and normalize for case-insensitive comparison
        table1 = self._extract_table_name(stmt1).lower()
        table2 = self._extract_table_name(stmt2).lower()

        # If tables are different, they can't be merged
        if table1 != table2:
            return False

        cols1 = self._extract_columns(stmt1)
        cols2 = self._extract_columns(stmt2)

        # If either statement has implicit columns, they're compatible
        if not cols1 or not cols2:
            return True

        # Split and normalize column names
        cols1_list = [c.strip('`"\'').lower() for c in cols1.strip('()').split(',')]
        cols2_list = [c.strip('`"\'').lower() for c in cols2.strip('()').split(',')]

        # Check if columns match (order doesn't matter)
        return len(cols1_list) == len(cols2_list) and set(cols1_list) == set(cols2_list)

    def merge(self, statements: List[str]) -> List[str]:
        """Merge multiple SQL INSERT statements.

        Args:
            statements: List of SQL statements to merge.

        Returns:
            List of merged SQL statements.

        Raises:
            InsertMergerError: If statements cannot be merged.
        """
        if not statements:
            raise InsertMergerError("No statements to merge")

        # Validate all statements
        for stmt in statements:
            self._validate_sql(stmt)

        # Group statements by table and columns
        table_groups = {}
        for stmt in statements:
            table = self._extract_table_name(stmt)
            cols = self._extract_columns(stmt)
            key = (table.lower(), cols.lower() if cols else None)
            
            # Find compatible group
            found = False
            for group_key in table_groups:
                if self._are_compatible(table_groups[group_key][0], stmt):
                    table_groups[group_key].append(stmt)
                    found = True
                    break
            
            if not found:
                table_groups[key] = [stmt]

        # Merge each group
        result = []
        for stmts in table_groups.values():
            # Extract components from first statement
            base_stmt = stmts[0]
            table = self._extract_table_name(base_stmt)
            columns = self._extract_columns(base_stmt)
            
            # Extract and merge values
            values = []
            for stmt in stmts:
                stmt_values = self._extract_values(stmt)
                if stmt_values:
                    values.append(stmt_values)

            # Construct merged statement
            merged = f"INSERT INTO {table}"
            if columns:
                merged += f" {columns}"
            merged += " VALUES " + self._merge_values(values)

            # Check size limit
            if len(merged) > self.max_bytes:
                # If single statement exceeds limit, return as is
                if len(stmts) == 1:
                    result.append(stmts[0])
                    continue
                # Otherwise, split into smaller batches
                mid = len(stmts) // 2
                result.extend(self.merge(stmts[:mid]))
                result.extend(self.merge(stmts[mid:]))
            else:
                result.append(merged)

        return result

    def add_statement(self, sql: str) -> Optional[List[str]]:
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

    def flush_table(self, table: str) -> List[str]:
        """Flush statements for a specific table.

        Args:
            table: Table name to flush.

        Returns:
            List of merged SQL statements.
        """
        if table not in self.table_maps or not self.table_maps[table]['statements']:
            return []

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

    def _merge_values(self, values_list: List[str]) -> str:
        """Merge multiple VALUES clauses into a single clause.

        Args:
            values_list: List of VALUES clauses to merge.

        Returns:
            Merged VALUES clause.
        """
        if not values_list:
            return ""

        # Split values on top-level commas
        all_values = []
        for values in values_list:
            paren_count = 0
            in_quote = None
            current = []
            start = 0
            
            for i, char in enumerate(values):
                # Handle quotes
                if char in '`"\'' and (not in_quote or char == in_quote):
                    in_quote = None if in_quote else char
                    continue
                    
                if not in_quote:
                    if char == '(':
                        paren_count += 1
                        if paren_count == 1:
                            start = i
                    elif char == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            value = values[start:i+1].strip()
                            if value:
                                current.append(value)
            
            all_values.extend(current)
            
        # Combine all values with proper formatting
        return ', '.join(all_values)
