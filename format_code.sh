#!/bin/bash
# Script to format code using black and isort
set -e

echo "Running isort..."
isort src/sql_batcher tests examples

echo "Running black..."
black src/sql_batcher tests examples

echo "All files formatted successfully."