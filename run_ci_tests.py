#!/usr/bin/env python
"""
Simple test runner for CI environments.

This script provides a simplified way to run tests in CI environments,
focusing on reliability and simplicity.
"""
import argparse
import subprocess
import sys


def run_core_tests(coverage=False, verbose=False):
    """Run core tests that don't require database connections."""
    cmd = ["pytest"]
    
    # Add test paths
    cmd.extend([
        "tests/test_batcher.py",
        "tests/test_adapters.py::TestSQLAdapter",
        "tests/test_adapters.py::TestGenericAdapter",
        "tests/test_query_collector_coverage.py",
        "tests/test_insert_merger.py",
        "tests/test_insert_merging_config.py",
        "tests/test_retry.py",
        "tests/test_retry_coverage.py",
        "tests/test_async_batcher.py",
        "tests/test_async_insert_merging.py",
        "tests/test_batcher_coverage.py",
        "tests/test_sql_batcher_insert_merging.py",
        "tests/test_postgresql_adapter_mock.py",
        "tests/test_trino_adapter_mock.py",
        "tests/test_async_postgresql_adapter_simple_mock.py"
    ])
    
    # Add options
    if coverage:
        cmd.extend(["--cov=sql_batcher", "--cov-report=xml", "--cov-report=term"])
    
    if verbose:
        cmd.append("-v")
    
    print(f"Running core tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def run_postgresql_tests(coverage=False, verbose=False):
    """Run PostgreSQL adapter tests."""
    cmd = ["pytest", "tests/test_postgresql_adapter.py"]
    
    # Add options
    if coverage:
        cmd.extend(["--cov=sql_batcher.adapters.postgresql", "--cov-report=xml", "--cov-report=term"])
    
    if verbose:
        cmd.append("-v")
    
    print(f"Running PostgreSQL tests: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run SQL Batcher tests for CI")
    parser.add_argument("--core", action="store_true", help="Run core tests")
    parser.add_argument("--postgres", action="store_true", help="Run PostgreSQL tests")
    parser.add_argument("--coverage", action="store_true", help="Collect test coverage information")
    parser.add_argument("--verbose", "-v", action="store_true", help="Run tests with verbose output")
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    exit_codes = []
    
    if args.core:
        exit_codes.append(run_core_tests(coverage=args.coverage, verbose=args.verbose))
    
    if args.postgres:
        exit_codes.append(run_postgresql_tests(coverage=args.coverage, verbose=args.verbose))
    
    # If no specific tests were requested, run all
    if not (args.core or args.postgres):
        exit_codes.append(run_core_tests(coverage=args.coverage, verbose=args.verbose))
    
    # Return non-zero if any test failed
    if any(code != 0 for code in exit_codes):
        sys.exit(1)
    else:
        print("All selected tests passed!")


if __name__ == "__main__":
    main()
