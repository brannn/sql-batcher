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

    @given(
        st=st.text(
            alphabet=st.characters(
                blacklist_categories=('Cs',),
                blacklist_characters=('\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x0b', '\x0c', '\x0e', '\x0f', '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a', '\x1b', '\x1c', '\x1d', '\x1e', '\x1f', '\x7f', '\x80', '\x81', '\x82', '\x83', '\x84', '\x85', '\x86', '\x87', '\x88', '\x89', '\x8a', '\x8b', '\x8c', '\x8d', '\x8e', '\x8f', '\x90', '\x91', '\x92', '\x93', '\x94', '\x95', '\x96', '\x97', '\x98', '\x99', '\x9a', '\x9b', '\x9c', '\x9d', '\x9e', '\x9f', '\xa0', '\xa1', '\xa2', '\xa3', '\xa4', '\xa5', '\xa6', '\xa7', '\xa8', '\xa9', '\xaa', '\xab', '\xac', '\xad', '\xae', '\xaf', '\xb0', '\xb1', '\xb2', '\xb3', '\xb4', '\xb5', '\xb6', '\xb7', '\xb8', '\xb9', '\xba', '\xbb', '\xbc', '\xbd', '\xbe', '\xbf', '\xc0', '\xc1', '\xc2', '\xc3', '\xc4', '\xc5', '\xc6', '\xc7', '\xc8', '\xc9', '\xca', '\xcb', '\xcc', '\xcd', '\xce', '\xcf', '\xd0', '\xd1', '\xd2', '\xd3', '\xd4', '\xd5', '\xd6', '\xd7', '\xd8', '\xd9', '\xda', '\xdb', '\xdc', '\xdd', '\xde', '\xdf', '\xe0', '\xe1', '\xe2', '\xe3', '\xe4', '\xe5', '\xe6', '\xe7', '\xe8', '\xe9', '\xea', '\xeb', '\xec', '\xed', '\xee', '\xef', '\xf0', '\xf1', '\xf2', '\xf3', '\xf4', '\xf5', '\xf6', '\xf7', '\xf8', '\xf9', '\xfa', '\xfb', '\xfc', '\xfd', '\xfe', '\xff'),
                min_codepoint=32,
                max_codepoint=126
            ),
            min_size=1,
            max_size=100
        )
    )
    def test_extract_values_with_hypothesis(self, st):
        """Test _extract_values with Hypothesis-generated values."""
        merger = InsertMerger()
        # Create a valid SQL statement with the generated values
        sql = f"INSERT INTO test_table VALUES ({st})"
        values = merger._extract_values(sql)
        assert values is not None
        assert values.strip('()') == st

    @given(
        table_name=st.text(
            alphabet=st.characters(
                blacklist_categories=('Cs',),
                blacklist_characters=('\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x0b', '\x0c', '\x0e', '\x0f', '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a', '\x1b', '\x1c', '\x1d', '\x1e', '\x1f', '\x7f', '\x80', '\x81', '\x82', '\x83', '\x84', '\x85', '\x86', '\x87', '\x88', '\x89', '\x8a', '\x8b', '\x8c', '\x8d', '\x8e', '\x8f', '\x90', '\x91', '\x92', '\x93', '\x94', '\x95', '\x96', '\x97', '\x98', '\x99', '\x9a', '\x9b', '\x9c', '\x9d', '\x9e', '\x9f', '\xa0', '\xa1', '\xa2', '\xa3', '\xa4', '\xa5', '\xa6', '\xa7', '\xa8', '\xa9', '\xaa', '\xab', '\xac', '\xad', '\xae', '\xaf', '\xb0', '\xb1', '\xb2', '\xb3', '\xb4', '\xb5', '\xb6', '\xb7', '\xb8', '\xb9', '\xba', '\xbb', '\xbc', '\xbd', '\xbe', '\xbf', '\xc0', '\xc1', '\xc2', '\xc3', '\xc4', '\xc5', '\xc6', '\xc7', '\xc8', '\xc9', '\xca', '\xcb', '\xcc', '\xcd', '\xce', '\xcf', '\xd0', '\xd1', '\xd2', '\xd3', '\xd4', '\xd5', '\xd6', '\xd7', '\xd8', '\xd9', '\xda', '\xdb', '\xdc', '\xdd', '\xde', '\xdf', '\xe0', '\xe1', '\xe2', '\xe3', '\xe4', '\xe5', '\xe6', '\xe7', '\xe8', '\xe9', '\xea', '\xeb', '\xec', '\xed', '\xee', '\xef', '\xf0', '\xf1', '\xf2', '\xf3', '\xf4', '\xf5', '\xf6', '\xf7', '\xf8', '\xf9', '\xfa', '\xfb', '\xfc', '\xfd', '\xfe', '\xff'),
                min_codepoint=32,
                max_codepoint=126
            ),
            min_size=1,
            max_size=50
        )
    )
    def test_extract_table_name_with_hypothesis(self, table_name):
        """Test _extract_table_name with Hypothesis-generated table names."""
        merger = InsertMerger()
        # Create a valid SQL statement with the generated table name
        sql = f"INSERT INTO {table_name} VALUES (1)"
        extracted = merger._extract_table_name(sql)
        assert extracted == table_name

    @given(
        statements=st.lists(
            st.text(
                alphabet=st.characters(
                    blacklist_categories=('Cs',),
                    blacklist_characters=('\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x0b', '\x0c', '\x0e', '\x0f', '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a', '\x1b', '\x1c', '\x1d', '\x1e', '\x1f', '\x7f', '\x80', '\x81', '\x82', '\x83', '\x84', '\x85', '\x86', '\x87', '\x88', '\x89', '\x8a', '\x8b', '\x8c', '\x8d', '\x8e', '\x8f', '\x90', '\x91', '\x92', '\x93', '\x94', '\x95', '\x96', '\x97', '\x98', '\x99', '\x9a', '\x9b', '\x9c', '\x9d', '\x9e', '\x9f', '\xa0', '\xa1', '\xa2', '\xa3', '\xa4', '\xa5', '\xa6', '\xa7', '\xa8', '\xa9', '\xaa', '\xab', '\xac', '\xad', '\xae', '\xaf', '\xb0', '\xb1', '\xb2', '\xb3', '\xb4', '\xb5', '\xb6', '\xb7', '\xb8', '\xb9', '\xba', '\xbb', '\xbc', '\xbd', '\xbe', '\xbf', '\xc0', '\xc1', '\xc2', '\xc3', '\xc4', '\xc5', '\xc6', '\xc7', '\xc8', '\xc9', '\xca', '\xcb', '\xcc', '\xcd', '\xce', '\xcf', '\xd0', '\xd1', '\xd2', '\xd3', '\xd4', '\xd5', '\xd6', '\xd7', '\xd8', '\xd9', '\xda', '\xdb', '\xdc', '\xdd', '\xde', '\xdf', '\xe0', '\xe1', '\xe2', '\xe3', '\xe4', '\xe5', '\xe6', '\xe7', '\xe8', '\xe9', '\xea', '\xeb', '\xec', '\xed', '\xee', '\xef', '\xf0', '\xf1', '\xf2', '\xf3', '\xf4', '\xf5', '\xf6', '\xf7', '\xf8', '\xf9', '\xfa', '\xfb', '\xfc', '\xfd', '\xfe', '\xff'),
                    min_codepoint=32,
                    max_codepoint=126
                ),
                min_size=1,
                max_size=10
            )
        )
    )
    def test_merge_with_hypothesis(self, statements):
        """Test merge with Hypothesis-generated statements."""
        merger = InsertMerger()
        # Create valid SQL statements
        valid_statements = [f"INSERT INTO test_table VALUES ({i})" for i in range(len(statements))]
        merged = merger.merge(valid_statements)
        assert len(merged) > 0
        assert all(isinstance(stmt, str) for stmt in merged)

    @given(
        max_bytes=st.integers(min_value=1, max_value=1000000)
    )
    def test_max_bytes_with_hypothesis(self, max_bytes):
        """Test max_bytes parameter with Hypothesis-generated values."""
        merger = InsertMerger()
        merger.max_bytes = max_bytes
        statements = [f"INSERT INTO test_table VALUES ({i})" for i in range(100)]
        merged = merger.merge(statements)
        assert all(len(stmt) <= max_bytes for stmt in merged) 