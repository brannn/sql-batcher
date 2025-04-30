# Changelog

All notable changes to SQL Batcher will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- TBD

### Changed
- TBD

### Fixed
- TBD

## [0.1.4] - 2024-04-29

### Changed
- Reorganized repository structure for a cleaner root directory
- Moved script files to scripts/ directory
- Moved configuration files to config/ directory
- Moved documentation files to docs/ directory
- Added README files to scripts/ and config/ directories

## [0.1.3] - 2024-04-30

### Changed
- Separated CI and publishing workflows

### Fixed
- Configured trusted publishing for PyPI

## [0.1.2] - 2024-04-30

### Fixed
- Fixed license configuration in pyproject.toml

## [0.1.1] - 2024-04-28

### Added
- GitHub Actions CI workflow for automated testing and publishing
- Comprehensive test framework with support for all database adapters
- Better support for running tests without available database connections
- Enhanced documentation including testing guidelines
- PyPI packaging configuration

### Changed
- Improved README with clearer value proposition
- Updated pre-commit configuration to include flake8
- Improved test coverage for PostgreSQL and Trino adapters
- Consolidated testing on pytest, removing unittest style assertions
- Better error handling in batch processing for oversized statements

### Fixed
- Issue with PostgreSQL COPY command and proper cursor handling
- Test failures in PostgreSQL adapter tests when mocking the connection

## [0.1.0] - 2024-04-26

### Added
- Core SQLBatcher implementation with batching based on size limits
- Support for dynamic column-based batch size adjustment
- Query collector interface for analyzing generated SQL
- Abstract adapter interface for database-specific implementations
- Implementations for multiple database systems:
  - Generic adapter for any database connection
  - PostgreSQL adapter with COPY command optimization
  - Trino adapter with session property support
  - Snowflake adapter with parameterized queries
  - BigQuery adapter with job monitoring
  - Spark adapter for distributed SQL processing
- Dry run mode for inspecting batches without execution
- Comprehensive examples for all supported databases
- Type annotations throughout the codebase
- Documentation with installation and usage instructions
- Test infrastructure with pytest markers for different database systems