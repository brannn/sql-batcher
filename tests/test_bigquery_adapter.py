from typing import Any, Tuple
from unittest.mock import MagicMock

import pytest

# Mark all tests in this file as using bigquery-specific functionality
pytestmark = [pytest.mark.db, pytest.mark.bigquery]

from sql_batcher.adapters.bigquery import BigQueryAdapter


def setup_mock_bq_connection(mocker: Any) -> Tuple[Any, Any]:
    """Set up mock BigQuery client and job."""
    mock_job = mocker.Mock()
    mock_client = mocker.Mock()
    mock_client.query.return_value = mock_job
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)
    return mock_client, mock_job


@pytest.fixture
def mock_bq(mocker: Any) -> Tuple[BigQueryAdapter, Any, Any]:
    """Create a mock BigQuery adapter with mocked client and job."""
    client, job = setup_mock_bq_connection(mocker)
    adapter = BigQueryAdapter(
        project_id="test-project", dataset_id="test_dataset", use_batch_mode=True
    )
    return adapter, client, job


def test_bq_execute(mock_bq: Tuple[BigQueryAdapter, Any, Any]) -> None:
    """Test executing a query with BigQuery adapter."""
    adapter, client, job = mock_bq
    job.result.return_value = [(1, "test1"), (2, "test2"), (3, "test3")]

    result = adapter.execute("SELECT * FROM test_table")
    assert result == [(1, "test1"), (2, "test2"), (3, "test3")]
    client.query.assert_called_once()


def test_bq_execute_no_results(mock_bq: Tuple[BigQueryAdapter, Any, Any]) -> None:
    """Test executing a non-SELECT query with BigQuery adapter."""
    adapter, client, job = mock_bq
    job.result.return_value = []

    result = adapter.execute("CREATE TABLE test_table (id INT64)")
    assert result == []
    client.query.assert_called_once()


def test_bq_batch_mode(mocker: Any) -> None:
    """Test batch mode configuration in BigQuery adapter."""
    client, _ = setup_mock_bq_connection(mocker)

    adapter = BigQueryAdapter(
        project_id="test-project", dataset_id="test_dataset", use_batch_mode=True
    )

    adapter.execute("SELECT * FROM test_table")

    # Verify that batch mode configuration was used
    client.query.assert_called_once()
    job_config = client.query.call_args[1]["job_config"]
    assert job_config.priority == "BATCH"


def test_bq_dataset_location(mocker: Any) -> None:
    """Test dataset location configuration in BigQuery adapter."""
    client, _ = setup_mock_bq_connection(mocker)

    adapter = BigQueryAdapter(
        project_id="test-project", dataset_id="test_dataset", location="US"
    )

    adapter.execute("SELECT * FROM test_table")

    # Verify that location was used
    client.query.assert_called_once()
    job_config = client.query.call_args[1]["job_config"]
    assert job_config.location == "US"


class TestBigQueryAdapter:
    """Test cases for BigQueryAdapter class."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch) -> None:
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
        monkeypatch.setattr(
            "sql_batcher.adapters.bigquery.bigquery", self.mock_bigquery
        )

        # Create the adapter
        self.adapter = BigQueryAdapter(
            project_id="test-project", dataset_id="test_dataset", location="US"
        )

    def test_init(self) -> None:
        """Test initialization."""
        # Check that the client was created with the correct parameters
        self.mock_bigquery.Client.assert_called_once_with(
            project="test-project", location="US"
        )

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
        empty_mock_result = []
        self.mock_query_job.result.return_value = empty_mock_result

        # Execute an INSERT statement
        result = self.adapter.execute(
            "INSERT INTO `test_dataset.users` (id, name) VALUES (3, 'New User')"
        )

        # Verify the query was executed
        self.mock_client.query.assert_called_once()
        args, kwargs = self.mock_client.query.call_args
        assert (
            args[0]
            == "INSERT INTO `test_dataset.users` (id, name) VALUES (3, 'New User')"
        )

        # Verify result
        assert result == []

    def test_execute_batch(self) -> None:
        """Test executing in batch mode."""
        # Create an adapter with batch_mode=True
        batch_adapter = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US",
            batch_mode=True,
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
        """Test max query size in batch mode."""
        # Create an adapter with batch_mode=True
        batch_adapter = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US",
            batch_mode=True,
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
        """Test getting a dataset reference."""
        # Setup mock dataset reference
        mock_dataset_ref = MagicMock()
        self.mock_client.dataset.return_value = mock_dataset_ref

        # Get the dataset reference
        result = self.adapter._get_dataset_reference()

        # Verify the client.dataset method was called with the correct ID
        self.mock_client.dataset.assert_called_once_with(
            "test_dataset", project="test-project"
        )

        # Verify the result
        assert result == mock_dataset_ref

    def test_table_reference(self) -> None:
        """Test getting a table reference."""
        # Setup mock references
        mock_dataset_ref = MagicMock()
        mock_table_ref = MagicMock()
        self.mock_client.dataset.return_value = mock_dataset_ref
        mock_dataset_ref.table.return_value = mock_table_ref

        # Get the table reference
        result = self.adapter._get_table_reference("users")

        # Verify the dataset reference was retrieved
        self.mock_client.dataset.assert_called_once_with(
            "test_dataset", project="test-project"
        )

        # Verify the table method was called
        mock_dataset_ref.table.assert_called_once_with("users")

        # Verify the result
        assert result == mock_table_ref

    def test_get_query_job_config(self) -> None:
        """Test creating a query job configuration."""
        # Get a query job config
        result = self.adapter._get_query_job_config()

        # Verify the QueryJobConfig was created
        self.mock_bigquery.QueryJobConfig.assert_called_once()

        # Verify the result
        assert result == self.mock_job_config

        # Verify priority is set to INTERACTIVE by default
        assert result.priority == self.mock_bigquery.QueryPriority.INTERACTIVE

    def test_execute_with_job_labels(self) -> None:
        """Test execution with job labels."""
        # Create an adapter with job labels
        adapter_with_labels = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US",
            job_labels={"department": "analytics", "application": "sql-batcher"},
        )

        # Reset the mock
        self.mock_client.reset_mock()
        self.mock_bigquery.QueryJobConfig.reset_mock()

        # Execute a query
        adapter_with_labels.execute("SELECT 1")

        # Verify the job config was created with labels
        args, kwargs = self.mock_client.query.call_args
        assert kwargs["job_config"].labels == {
            "department": "analytics",
            "application": "sql-batcher",
        }

    def test_missing_bigquery_package(self, monkeypatch) -> None:
        """Test behavior when google.cloud.bigquery package is not installed."""
        # Simulate the bigquery package not being installed
        monkeypatch.setattr("sql_batcher.adapters.bigquery.bigquery", None)

        # Attempting to create the adapter should raise an ImportError
        with pytest.raises(ImportError) as excinfo:
            BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Verify the error message
        assert "google-cloud-bigquery package is required" in str(excinfo.value)
