#!/bin/bash
# Script to format code using autoflake, black, and isort
set -e

echo "Running autoflake to remove unused imports..."
autoflake --recursive --remove-all-unused-imports --remove-unused-variables --in-place src/sql_batcher tests examples

echo "Running isort..."
isort src/sql_batcher tests examples

echo "Running black..."
black src/sql_batcher tests examples

echo "Running flake8 (warnings only)..."
flake8 src/sql_batcher tests examples || echo "Flake8 found some issues, but continuing..."

echo "All files formatted successfully."