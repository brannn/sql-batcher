# SQL Batcher Scripts

This directory contains utility scripts for development, testing, and maintenance of the SQL Batcher project.

## Development Scripts

- `format_code.sh` - Formats code using autoflake, black, and isort
- `fix_flake8.py` - Fixes common flake8 issues in the codebase
- `setup-hooks.sh` - Sets up Git hooks for the repository

## Testing Scripts

- `run_ci_tests.py` - Simplified test runner for CI environments
- `run_full_tests.py` - Comprehensive test runner with intelligent adapter handling
- `run_sqlbatcher_tests.sh` - Simple script to run core SQL Batcher tests

## Usage

Most scripts can be run directly from the command line. For example:

```bash
# Format code
./scripts/format_code.sh

# Run core tests
python ./scripts/run_ci_tests.py --core

# Run full tests
python ./scripts/run_full_tests.py --all
```

For more information about each script, refer to the documentation within the script itself.
