"""
SQL Batcher - Efficiently batch SQL statements based on size limits

SQL Batcher addresses a common challenge in database programming: efficiently
executing many SQL statements while respecting query size limitations. It's
especially valuable for systems like Trino and Snowflake that
have query size or memory constraints.

Examples:
    Basic usage:

    >>> from sql_batcher import SQLBatcher
    >>> batcher = SQLBatcher(max_bytes=1000000)
    >>> statements = [
    ...     "INSERT INTO users VALUES (1, 'Alice')",
    ...     "INSERT INTO users VALUES (2, 'Bob')"
    ... ]
    >>> def execute_sql(sql):
    ...     print(f"Executing: {sql}")
    ...     # In a real scenario, this would execute the SQL
    >>> batcher.process_statements(statements, execute_sql)
"""

from sql_batcher.async_batcher import AsyncSQLBatcher
from sql_batcher.batcher import SQLBatcher

__version__ = "0.1.4"


__all__ = ["SQLBatcher", "AsyncSQLBatcher"]
