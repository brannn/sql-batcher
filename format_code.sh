#!/bin/bash
# Script to format code using black and isort
set -e

echo "Running isort..."
isort src/sql_batcher tests examples docs

echo "Running black..."
black src/sql_batcher tests examples docs

echo "All files formatted successfully."