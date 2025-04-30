# Contributing to SQL Batcher

First off, thank you for considering contributing to SQL Batcher! It's people like you that make SQL Batcher such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by the SQL Batcher Code of Conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report. Following these guidelines helps maintainers and the community understand your report, reproduce the behavior, and find related reports.

Before creating bug reports, please check the existing issues list as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:

* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as many details as possible.
* **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy-pasteable snippets, which you use in those examples.
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**
* **Include screenshots and animated GIFs** which show you following the described steps and clearly demonstrate the problem.
* **If the problem is related to performance or memory**, include a CPU profile capture with your report.
* **If the problem wasn't triggered by a specific action**, describe what you were doing before the problem happened.

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion, including completely new features and minor improvements to existing functionality.

* **Use a clear and descriptive title** for the issue to identify the suggestion.
* **Provide a step-by-step description of the suggested enhancement** in as many details as possible.
* **Provide specific examples to demonstrate the steps** or point to similar examples in other projects.
* **Describe the current behavior** and **explain which behavior you expected to see instead** and why.
* **Explain why this enhancement would be useful** to most SQL Batcher users.
* **List some other applications where this enhancement exists.**

### Pull Requests

The process described here has several goals:

- Maintain SQL Batcher's quality
- Fix problems that are important to users
- Engage the community in working toward the best possible SQL Batcher
- Enable a sustainable system for SQL Batcher's maintainers to review contributions

Please follow these steps to have your contribution considered by the maintainers:

1. Follow all instructions in the template
2. Follow the [styleguides](#styleguides)
3. After you submit your pull request, verify that all [status checks](https://help.github.com/articles/about-status-checks/) are passing

## Styleguides

### Git Commit Messages

* Use the present tense ("Add feature" not "Added feature")
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
* Limit the first line to 72 characters or less
* Reference issues and pull requests liberally after the first line
* When only changing documentation, include `[ci skip]` in the commit title

### Python Styleguide

All Python code is linted with:

* [Black](https://black.readthedocs.io/en/stable/) for code formatting
* [isort](https://pycqa.github.io/isort/) for import sorting
* [flake8](https://flake8.pycqa.org/en/latest/) for code style
* [mypy](https://mypy.readthedocs.io/en/stable/) for type checking

To ensure your code meets our style requirements, we use pre-commit hooks that automatically format and lint your code when you commit changes:

```bash
# Install pre-commit hooks
pip install pre-commit
pre-commit install
```

You can also run the hooks manually on all files:

```bash
pre-commit run --all-files
```

Or run individual tools manually:

```bash
# Format the code
black src/ tests/
isort src/ tests/

# Check for style issues
flake8 src/ tests/
mypy src/ tests/
```

### Documentation Styleguide

* Use [Markdown](https://daringfireball.net/projects/markdown) for documentation.
* Follow the conventions in existing documentation.
* Use descriptive link text instead of "here" or "this link."
* Include examples when adding new features.

## Setting Up Development Environment

To set up a development environment for SQL Batcher:

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```
   git clone https://github.com/your-username/sql-batcher.git
   cd sql-batcher
   ```
3. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install the package in development mode:
   ```
   pip install -e ".[dev,all]"
   ```
5. Install pre-commit hooks:
   ```
   pip install pre-commit
   pre-commit install
   ```
6. Create a branch for your feature:
   ```
   git checkout -b name-of-your-feature
   ```

## Testing

Please read the [TESTING.md](TESTING.md) guide for details on how the test suite is structured and how to add tests for your changes.

Basic test commands:

```bash
# Run core tests only (no database connections required)
python run_ci_tests.py --core

# Run PostgreSQL tests (requires PostgreSQL connection)
python run_ci_tests.py --postgres

# Run with test coverage reporting
python run_ci_tests.py --coverage
```

## Continuous Integration

SQL Batcher uses GitHub Actions for continuous integration. The CI workflow runs automatically on all pull requests and pushes to the main branch.

The CI workflow includes:

1. **Linting and formatting checks**: Ensures code follows our style guidelines using black, isort, flake8, and mypy.
2. **Core tests**: Runs tests that don't require database connections across multiple Python versions (3.8, 3.9, 3.10, 3.11).
3. **PostgreSQL tests**: Runs tests that require a PostgreSQL database using a containerized PostgreSQL instance.
4. **Package building**: Ensures the package can be built correctly.
5. **Publishing**: Automatically publishes new releases to PyPI when a new version tag is pushed.

You can see the CI workflow configuration in `.github/workflows/python-ci.yml`.

To run the CI checks locally before submitting a pull request:

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run linting checks
black --check src tests
isort --check src tests
flake8 src tests
mypy src tests

# Run tests
python run_ci_tests.py --core --coverage
```

## Additional Resources

* [General GitHub documentation](https://help.github.com/)
* [GitHub Pull Request documentation](https://help.github.com/articles/about-pull-requests/)