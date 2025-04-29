# Code Style Guide

This project follows a modified version of PEP 8 with some relaxed rules for better readability and developer experience.

## Code Formatting Tools

We use the following tools to maintain code quality:

1. **Black**: For consistent code formatting
2. **isort**: For organizing imports
3. **flake8**: For linting and style checking
4. **autoflake**: For removing unused imports and variables

## Style Guidelines

- **Line Length**: Maximum line length is 130 characters (relaxed from PEP 8's 79/88)
- **Imports**: Organized using isort with the black profile
- **Docstrings**: Google-style docstrings are preferred
- **Code Before Imports**: Allowed (e.g., for module-level docstrings, comments, etc.)

## Running the Formatter

You can format the code using the provided script:

```bash
./format_code.sh
```

This script will:
1. Remove unused imports and variables with autoflake
2. Sort imports with isort
3. Format code with black
4. Check for style issues with flake8 (warnings only)

## Pre-commit Hooks

We recommend using pre-commit hooks to automatically format code before committing:

```bash
# Install pre-commit
pip install pre-commit

# Install the git hooks
pre-commit install
```

## Configuration Files

- **pyproject.toml**: Contains configuration for black and isort
- **.flake8**: Contains configuration for flake8
- **.pre-commit-config.yaml**: Contains configuration for pre-commit hooks

## Exceptions

Some files or sections may be exempt from certain style rules. These exceptions are documented in the configuration files.
