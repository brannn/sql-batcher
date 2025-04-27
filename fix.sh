#!/bin/bash

# Adds `-> None` to any function definitions in tests/ that are missing a return type
# (Very basic but effective for test files)

echo "Adding '-> None' to untyped test functions..."

find tests -name "*.py" | while read -r file; do
  echo "Processing $file"
  # Use sed to insert "-> None" in function definitions missing a return type
  sed -i.bak -E 's/^(def [a-zA-Z0-9_]+\([^)]*\)):/\1 -> None:/g' "$file"
done

echo "Done! Created backup .bak files in case you want to review changes."

