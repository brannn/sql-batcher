from typing import Any, List, Tuple
from unittest.mock import MagicMock

import pytest

from sql_batcher.adapters.bigquery import BigQueryAdapter

# Mark all tests in this file as using bigquery-specific functionality
pytestmark = [pytest.mark.db, pytest.mark.bigquery]


def setup_mock_bq_connection() -> Tuple[MagicMock, MagicMock]:
    """Set up a mock BigQuery connection and cursor."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Configure mock cursor
    mock_cursor.description = [
        ["id", "INT64", None, None, None, None, None],
        ["name", "STRING", None, None, None, None, None],
    ]
    mock_cursor.fetchall.return_value = [(1, "Test")]

    return mock_connection, mock_cursor


@pytest.fixture
def mock_bq() -> Tuple[BigQueryAdapter, MagicMock, MagicMock]:
    """Create a mock BigQuery adapter."""
    mock_connection, mock_cursor = setup_mock_bq_connection()

    # Create adapter with mock connection
    adapter = BigQueryAdapter(
        project_id="test-project",
        dataset_id="test_dataset",
        location="US",
    )
    adapter._connection = mock_connection
    adapter._cursor = mock_cursor

    return adapter, mock_connection, mock_cursor


def test_bq_execute(mock_bq: Tuple[BigQueryAdapter, Any, Any]) -> None:
    """Test basic execution."""
    adapter, _, cursor = mock_bq

    # Execute a query
    results = adapter.execute("SELECT * FROM test")

    # Should return results
    assert len(results) == 1
    assert results[0][0] == 1
    assert results[0][1] == "Test"

    # Should call cursor.execute
    cursor.execute.assert_called_once_with("SELECT * FROM test")


def test_bq_execute_no_results(mock_bq: Tuple[BigQueryAdapter, Any, Any]) -> None:
    """Test execution with no results."""
    adapter, _, cursor = mock_bq

    # Configure cursor to return no results
    cursor.description = None
    cursor.fetchall.return_value = []

    # Execute an insert
    results = adapter.execute("CREATE TABLE test (id INT64, name STRING)")

    # Should not return results
    assert len(results) == 0


def test_bq_batch_mode(mock_bq: Tuple[BigQueryAdapter, Any, Any]) -> None:
    """Test batch mode execution."""
    adapter, _, _ = mock_bq

    # Should use batch mode
    assert adapter.get_max_query_size() == 1000000


def test_bq_dataset_location(mock_bq: Tuple[BigQueryAdapter, Any, Any]) -> None:
    """Test dataset location."""
    adapter, _, _ = mock_bq

    # Should set dataset location
    assert adapter._location == "US"


def test_bq_labels(mock_bq: Tuple[BigQueryAdapter, Any, Any]) -> None:
    """Test job labels."""
    adapter, _, _ = mock_bq

    # Create adapter with labels
    labels = {"env": "test", "team": "data"}
    adapter = BigQueryAdapter(
        project_id="test-project",
        dataset_id="test_dataset",
        location="US",
        labels=labels,
    )

    # Should set job labels
    assert adapter.job_config.labels == labels


class TestBigQueryAdapter:
    """Test cases for BigQueryAdapter class."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch: Any) -> None:
        """Set up test fixtures."""
        # Mock the google.cloud.bigquery module
        self.mock_bigquery = MagicMock()
        self.mock_client = MagicMock()
        self.mock_job = MagicMock()
        self.mock_job_config = MagicMock()
        self.mock_query_job = MagicMock()

        # Configure the mocks
        self.mock_bigquery.Client.return_value = self.mock_client
        self.mock_bigquery.QueryJobConfig.return_value = self.mock_job_config
        self.mock_client.query.return_value = self.mock_query_job

        # Mock query results
        self.mock_query_job.result.return_value = [
            MagicMock(id=1, name="Test User"),
            MagicMock(id=2, name="Another User"),
        ]

        # Patch the required modules
        monkeypatch.setattr("sql_batcher.adapters.bigquery.bigquery", self.mock_bigquery)

        # Create the adapter
        self.adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset", location="US")

    def test_init(self) -> None:
        """Test initialization."""
        # Check that the client was created with the correct parameters
        self.mock_bigquery.Client.assert_called_once_with(project="test-project", location="US")

        # Check that the adapter has the correct properties
        assert self.adapter._client == self.mock_client
        assert self.adapter._dataset_id == "test_dataset"
        assert self.adapter._project_id == "test-project"
        assert self.adapter._location == "US"

    def test_get_max_query_size(self) -> None:
        """Test get_max_query_size method."""
        # BigQuery has different limits for interactive (1MB) and batch (20MB) queries
        # The adapter defaults to the interactive limit
        assert self.adapter.get_max_query_size() == 1_000_000

    def test_execute_select(self) -> None:
        """Test executing a SELECT statement."""
        # Execute a SELECT statement
        result = self.adapter.execute("SELECT id, name FROM `test_dataset.users`")

        # Verify the query was executed with the correct SQL
        self.mock_client.query.assert_called_once()
        args, kwargs = self.mock_client.query.call_args
        assert args[0] == "SELECT id, name FROM `test_dataset.users`"
        assert "job_config" in kwargs

        # Verify query result was processed
        self.mock_query_job.result.assert_called_once()

        # Check the returned data
        assert len(result) == 2
        assert result[0].id == 1
        assert result[0].name == "Test User"
        assert result[1].id == 2
        assert result[1].name == "Another User"

    def test_execute_insert(self) -> None:
        """Test executing an INSERT statement."""
        # Setup mock to return an empty result for non-SELECT queries
        empty_mock_result: List[Any] = []
        self.mock_query_job.result.return_value = empty_mock_result

        # Execute an INSERT statement
        result = self.adapter.execute("INSERT INTO `test_dataset.users` (id, name) VALUES (3, 'New User')")

        # Verify the query was executed
        self.mock_client.query.assert_called_once()
        args, kwargs = self.mock_client.query.call_args
        assert args[0] == "INSERT INTO `test_dataset.users` (id, name) VALUES (3, 'New User')"

        # Verify result
        assert result == []

    def test_execute_batch(self) -> None:
        """Test executing in batch mode."""
        # Create an adapter with use_batch_mode=True
        batch_adapter = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US",
            use_batch_mode=True,
        )

        # Reset the mock
        self.mock_client.reset_mock()

        # Execute a query in batch mode
        batch_adapter.execute("SELECT * FROM `test_dataset.large_table`")

        # Verify the query was executed with appropriate batch settings
        self.mock_client.query.assert_called_once()
        args, kwargs = self.mock_client.query.call_args

        # In batch mode, priority should be set to BATCH
        assert kwargs["job_config"].priority == self.mock_bigquery.QueryPriority.BATCH

    def test_get_max_query_size_batch_mode(self) -> None:
        """Test get_max_query_size in batch mode."""
        # Create an adapter with use_batch_mode=True
        batch_adapter = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US",
            use_batch_mode=True,
        )

        # In batch mode, the limit should be 20MB
        assert batch_adapter.get_max_query_size() == 20_000_000

    def test_close(self) -> None:
        """Test closing the connection."""
        # Run the method
        self.adapter.close()

        # Verify the client was closed
        self.mock_client.close.assert_called_once()

    def test_dataset_reference(self) -> None:
        """Test dataset reference creation."""
        # Get the dataset reference
        dataset_ref = self.adapter._get_dataset_reference()

        # Verify the reference was created correctly
        assert dataset_ref.project == "test-project"
        assert dataset_ref.dataset_id == "test_dataset"

    def test_table_reference(self) -> None:
        """Test table reference creation."""
        # Get the table reference
        table_ref = self.adapter._get_table_reference("users")

        # Verify the reference was created correctly
        assert table_ref.project == "test-project"
        assert table_ref.dataset_id == "test_dataset"
        assert table_ref.table_id == "users"

    def test_get_query_job_config(self) -> None:
        """Test query job configuration."""
        # Get the job config
        job_config = self.adapter._get_query_job_config()

        # Verify the config was created correctly
        assert job_config.location == "US"
        assert job_config.priority == self.mock_bigquery.QueryPriority.INTERACTIVE

    def test_execute_with_job_labels(self) -> None:
        """Test execution with job labels."""
        # Create an adapter with job labels
        labels = {"environment": "test"}
        adapter_with_labels = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US",
            labels=labels,
        )

        # Execute a query
        adapter_with_labels.execute("SELECT 1")

        # Verify that labels were set
        job_config = self.mock_client.query.call_args[1]["job_config"]
        assert job_config.labels == {"environment": "test"}

    def test_missing_bigquery_package(self, monkeypatch: Any) -> None:
        """Test behavior when bigquery package is not installed."""
        # Remove the bigquery module
        monkeypatch.setattr("sql_batcher.adapters.bigquery.bigquery", None)

        # Attempt to create the adapter
        with pytest.raises(ImportError) as exc_info:
            BigQueryAdapter(
                project_id="test-project",
                dataset_id="test_dataset",
                location="US",
            )

        assert "google-cloud-bigquery package is required" in str(exc_info.value)
