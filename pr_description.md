# SQL Batcher Major Refactor

## Overview
This PR introduces a comprehensive refactor of the SQL Batcher library, adding async support, a plugin system, and significant improvements to the overall architecture. The changes include new features, enhanced testing, and improved documentation.

## Key Features

### 1. Async Support
- New AsyncSQLBatcher class for asynchronous operations
- Async adapters for multiple databases:
  - PostgreSQL
  - Snowflake
  - Trino
  - BigQuery
- Async-compatible batch management system
- Proper transaction and savepoint handling

### 2. Plugin System
- Flexible plugin architecture for extending functionality
- Hook system for customizing behavior
- Plugin lifecycle management
- Example plugins and documentation

### 3. Enhanced Query Management
- New QueryCollector class for efficient SQL statement handling
- Improved InsertMerger with better statement combining
- Batch size optimization and management
- Column-aware adjustments

### 4. Improved Testing
- Comprehensive test suite for async functionality
- Plugin system test coverage
- Integration tests for all adapters
- Mock adapters for testing
- Improved CI pipeline configuration

### 5. Documentation & Examples
- Added async usage examples
- Plugin system documentation
- Updated API documentation
- New example implementations

## Technical Details
- Added pytest-asyncio for async testing
- Implemented proper async context management
- Enhanced error handling and retry mechanisms
- Improved transaction management
- Better type hints and documentation

## Breaking Changes
None. All new features are backward compatible.

## Testing
- All tests passing
- Added new test cases for async functionality
- Enhanced coverage for core components
- Integration tests for all supported databases

## Documentation
- Added examples in examples/
- Updated docstrings
- Added architecture documentation
- Included plugin development guide

## Next Steps
- Review and merge
- Release new version
- Update documentation website 