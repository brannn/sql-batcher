"""
Property-based tests for the InsertMerger class using Hypothesis.
"""

import pytest
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.utils.insert_merger import InsertMerger
from sql_batcher.exceptions import InsertMergerError

# SQL-specific strategies
def sql_identifier():
    """Generate valid SQL identifiers."""
    return st.from_regex(r'[a-zA-Z_][a-zA-Z0-9_]{0,29}')

def sql_value():
    """Generate valid SQL values."""
    return st.one_of(
        st.integers(),
        st.floats(allow_infinity=False, allow_nan=False),
        st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P', 'Zs')), min_size=0, max_size=50),
        st.none()
    )

def sql_column_list():
    """Generate valid SQL column lists."""
    return st.lists(sql_identifier(), min_size=1, max_size=10, unique=True)

def sql_insert_statement():
    """Generate valid SQL INSERT statements."""
    return st.builds(
        lambda table, cols, vals: f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('NULL' if v is None else repr(v) for v in vals)})",
        sql_identifier(),
        sql_column_list(),
        st.lists(sql_value(), min_size=1, max_size=10)
    )

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

class TestInsertMerger:
    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_preserves_data(self, statements):
        """Test that merging preserves all data."""
        merger = InsertMerger()
        merged = merger.merge(statements)
        
        # Verify all original values appear in merged result
        for stmt in statements:
            assert stmt in merged

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_maintains_syntax(self, statements):
        """Test that merged statements maintain valid SQL syntax."""
        merger = InsertMerger()
        merged = merger.merge(statements)
        
        # Verify SQL syntax
        assert merged.startswith("INSERT INTO")
        assert "VALUES" in merged
        assert merged.count("(") == merged.count(")")

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_respects_size_limit(self, statements):
        """Test that merging respects max_bytes limit."""
        max_bytes = 1000
        merger = InsertMerger(max_bytes=max_bytes)
        merged = merger.merge(statements)
        
        # Verify size constraints
        assert len(merged) <= max_bytes

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_table_name_preservation(self, statements):
        """Test that table names are preserved in merged statements."""
        merger = InsertMerger()
        merged = merger.merge(statements)
        
        # Extract table names
        original_tables = {merger._extract_table_name(stmt) for stmt in statements}
        merged_table = merger._extract_table_name(merged)
        
        # Verify table names
        assert merged_table in original_tables

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_column_order_preservation(self, statements):
        """Test that column order is preserved in merged statements."""
        merger = InsertMerger()
        merged = merger.merge(statements)
        
        # Extract column lists
        original_cols = [merger._extract_columns(stmt) for stmt in statements]
        merged_cols = merger._extract_columns(merged)
        
        # Verify column order
        if original_cols[0]:  # If first statement has explicit columns
            assert merged_cols == original_cols[0]

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_value_preservation(self, statements):
        """Test that all values are preserved in merged statements."""
        merger = InsertMerger()
        merged = merger.merge(statements)
        
        # Extract values
        original_values = [merger._extract_values(stmt) for stmt in statements]
        merged_values = merger._extract_values(merged)
        
        # Verify values
        for val in original_values:
            assert val in merged_values

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_commutativity(self, statements):
        """Test that merging is commutative (order doesn't matter)."""
        merger = InsertMerger()
        
        # Merge in original order
        merged1 = merger.merge(statements)
        
        # Merge in reversed order
        merged2 = merger.merge(list(reversed(statements)))
        
        # Verify results are equivalent
        assert merged1 == merged2

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_associativity(self, statements):
        """Test that merging is associative (grouping doesn't matter)."""
        merger = InsertMerger()
        
        # Merge all at once
        merged1 = merger.merge(statements)
        
        # Merge in groups
        mid = len(statements) // 2
        merged2 = merger.merge([
            merger.merge(statements[:mid]),
            merger.merge(statements[mid:])
        ])
        
        # Verify results are equivalent
        assert merged1 == merged2

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_idempotence(self, statements):
        """Test that merging is idempotent (merging a merged statement doesn't change it)."""
        merger = InsertMerger()
        
        # First merge
        merged1 = merger.merge(statements)
        
        # Merge the merged result with itself
        merged2 = merger.merge([merged1, merged1])
        
        # Verify results are equivalent
        assert merged1 == merged2

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_preserves_whitespace(self, statements):
        """Test that merging preserves whitespace in values."""
        merger = InsertMerger()
        merged = merger.merge(statements)
        
        # Extract values
        original_values = [merger._extract_values(stmt) for stmt in statements]
        merged_values = merger._extract_values(merged)
        
        # Verify whitespace is preserved
        for val in original_values:
            assert val in merged_values

    @given(st.lists(sql_insert_statement(), min_size=1, max_size=10))
    def test_merge_preserves_case(self, statements):
        """Test that merging preserves case in identifiers."""
        merger = InsertMerger()
        merged = merger.merge(statements)
        
        # Extract identifiers
        original_ids = [merger._extract_table_name(stmt) for stmt in statements]
        merged_id = merger._extract_table_name(merged)
        
        # Verify case is preserved
        assert merged_id in original_ids 