# SQL Batcher Configuration Files

This directory contains configuration files for various development tools used in the SQL Batcher project.

## Configuration Files

- `.flake8` - Configuration for the flake8 linter
- `mypy.ini` - Configuration for the mypy type checker

## Usage

These configuration files are used by the corresponding tools when run from the project root directory. Symbolic links in the root directory point to these files for backward compatibility.

For example, to run flake8 with this configuration:

```bash
flake8 src tests
```

Or to run mypy with this configuration:

```bash
mypy src tests
```

These configurations are also used by pre-commit hooks and CI workflows.
