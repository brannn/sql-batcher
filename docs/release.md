# Release Process

This document outlines the process for creating and publishing a new release of SQL Batcher to PyPI.

## Prerequisites

1. Ensure you have the necessary permissions to publish to PyPI
2. Install the required tools:
   ```bash
   pip install build twine
   ```
3. Set up your PyPI credentials in `~/.pypirc` (see `.pypirc.template` for an example)

## Release Steps

### 1. Update Version

Update the version number in `pyproject.toml`:

```toml
[project]
name = "sql-batcher"
version = "x.y.z"  # Update this line
```

### 2. Update CHANGELOG.md

Ensure `CHANGELOG.md` is updated with all notable changes for this release:

```markdown
## [x.y.z] - YYYY-MM-DD

### Added
- New feature 1
- New feature 2

### Changed
- Change 1
- Change 2

### Fixed
- Bug fix 1
- Bug fix 2
```

### 3. Create a Git Tag

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "Bump version to x.y.z"
git tag -a vx.y.z -m "Version x.y.z"
git push origin main --tags
```

### 4. Build the Package

```bash
python -m build
```

This will create both source distribution (`.tar.gz`) and wheel (`.whl`) files in the `dist/` directory.

### 5. Test the Package (Optional but Recommended)

Upload to TestPyPI first:

```bash
twine upload --repository testpypi dist/*
```

Install from TestPyPI and verify it works:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sql-batcher==x.y.z
```

### 6. Publish to PyPI

```bash
twine upload dist/*
```

### 7. Verify the Release

- Check that the package is available on PyPI: https://pypi.org/project/sql-batcher/
- Install the package from PyPI and verify it works:
  ```bash
  pip install sql-batcher==x.y.z
  ```

## Troubleshooting

### Authentication Issues

If you encounter authentication issues, ensure your `.pypirc` file is correctly set up with valid API tokens.

### Package Validation Errors

If `twine` reports validation errors, fix the issues and rebuild the package before attempting to upload again.

### Version Conflicts

If the version already exists on PyPI, you cannot overwrite it. You must increment the version number and try again.
