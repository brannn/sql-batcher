# Testing SQL Batcher

SQL Batcher includes a comprehensive test suite to ensure reliability and correctness. This document provides guidance on running tests and contributing new tests.

## Test Structure

The test suite is organized into several categories:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test interactions between components
- **Adapter Tests**: Test database-specific adapters
- **Mock Tests**: Test with mocked database connections

## Running Tests

You can run the tests using pytest:

```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/test_sql_batcher.py

# Run with coverage
python -m pytest --cov=sql_batcher
```

## Database-Specific Tests

Some tests require specific database connections. You can enable these tests with command-line flags:

```bash
# Run PostgreSQL tests
python -m pytest --postgres

# Run Trino tests
python -m pytest --trino

# Run Snowflake tests
python -m pytest --snowflake

# Run BigQuery tests
python -m pytest --bigquery
```

## Mock Tests

For testing without actual database connections, we provide mock tests:

```bash
# Run mock adapter tests
python -m pytest tests/test_postgresql_adapter_mock.py tests/test_trino_adapter_mock.py
```

## Writing Tests

When writing tests for SQL Batcher, follow these guidelines:

1. **Use Mocks**: Prefer using mocks for database connections to avoid external dependencies
2. **Test Edge Cases**: Include tests for edge cases and error conditions
3. **Test Async Code**: For async components, use pytest-asyncio fixtures
4. **Test Transactions**: Include tests for transaction management
5. **Test Batching Logic**: Verify that batching works correctly with different statement types

### Example Test

```python
def test_batch_size_adjustment():
    """Test that batch size is adjusted based on column count."""
    # Create a mock adapter
    adapter = MockAdapter(max_query_size=1000)
    
    # Create a batcher with auto-adjustment enabled
    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=500,
        batch_mode=True,
        auto_adjust_for_columns=True
    )
    
    # Add statements with different column counts
    batcher.add_statement("INSERT INTO table1 (col1) VALUES (1)")
    batcher.add_statement("INSERT INTO table1 (col1, col2, col3) VALUES (1, 2, 3)")
    
    # Verify that batch size was adjusted
    assert batcher.get_current_batch_size() < 500
```

## Test Coverage

We aim for high test coverage to ensure reliability. You can check the current coverage with:

```bash
python -m pytest --cov=sql_batcher --cov-report=term-missing
```

## Continuous Integration

All tests are run in our CI pipeline to ensure code quality. Make sure your changes pass all tests before submitting a pull request.
