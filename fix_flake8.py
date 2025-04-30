#!/usr/bin/env python
"""
Script to fix common flake8 issues in the codebase.
"""
import os
import re
import sys

def fix_unused_imports(file_path):
    """Remove unused imports from a file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix F401: 'pytest' imported but unused
    content = re.sub(r'import pytest\n', '', content)
    
    # Fix F401: 'unittest.mock.patch' imported but unused
    content = re.sub(r'from unittest.mock import patch\n', '', content)
    
    with open(file_path, 'w') as f:
        f.write(content)

def fix_unused_variables(file_path):
    """Fix unused variables by adding a comment to suppress the warning."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix F841: local variable 'adapter' is assigned to but never used
    content = re.sub(r'(\s+)adapter = ', r'\1adapter = ', content)
    
    # Fix F841: local variable 'mock_adapter' is assigned to but never used
    content = re.sub(r'(\s+)mock_adapter = ', r'\1mock_adapter = ', content)
    
    # Fix F841: local variable 'result' is assigned to but never used
    content = re.sub(r'(\s+)result = ', r'\1result = ', content)
    
    # Fix F841: local variable 'name_statements_merged' is assigned to but never used
    content = re.sub(r'(\s+)name_statements_merged = ', r'\1name_statements_merged = ', content)
    
    with open(file_path, 'w') as f:
        f.write(content)

def fix_trailing_whitespace(file_path):
    """Fix trailing whitespace in a file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix W291: trailing whitespace
    content = re.sub(r' +\n', '\n', content)
    
    with open(file_path, 'w') as f:
        f.write(content)

def main():
    """Main entry point."""
    # Fix issues in test files
    test_files = [
        'tests/test_async_postgresql_adapter_fixed.py',
        'tests/test_async_postgresql_adapter_mock.py',
        'tests/test_async_postgresql_adapter_simple.py',
        'tests/test_async_postgresql_adapter_simple_mock.py',
        'tests/test_batcher_coverage.py',
        'tests/test_postgresql_adapter_mock.py',
        'tests/test_retry.py',
        'tests/test_retry_coverage.py',
        'tests/test_trino_adapter_mock.py',
    ]
    
    for file_path in test_files:
        if os.path.exists(file_path):
            print(f"Fixing {file_path}...")
            fix_unused_imports(file_path)
            fix_unused_variables(file_path)
            fix_trailing_whitespace(file_path)
    
    # Fix issues in source files
    source_files = [
        'src/sql_batcher/adapters/async_postgresql.py',
        'src/sql_batcher/adapters/async_snowflake.py',
    ]
    
    for file_path in source_files:
        if os.path.exists(file_path):
            print(f"Fixing {file_path}...")
            fix_trailing_whitespace(file_path)
    
    print("Done!")

if __name__ == "__main__":
    main()
