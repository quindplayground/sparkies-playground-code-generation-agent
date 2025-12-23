import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType

from quind_demo_ppd_project.flows.portafolios_dynamodb.job import portafolios_dynamodb_job
from quind_demo_ppd_project.libs.runner.types import Status


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource for testing."""
    mock_vars = MagicMock()
    mock_vars.vars.get = MagicMock(side_effect=lambda key, default=None: {
        "component_name": "portafolios_dynamodb",
        "last_snapshot_tag_name": "portafolios_dynamodb_last_snapshot",
    }.get(key, default))
    mock_vars.vars.input.table_id = "test_catalog.test_schema.test_table"
    mock_vars.vars.output.table_name = "test-output-table"
    mock_vars.vars.output.get = MagicMock(side_effect=lambda key, default=None: {
        "region": "us-east-1",
    }.get(key, default))
    mock_vars.vars.restart.get = MagicMock(return_value=False)
    return mock_vars


@pytest.fixture
def sample_extracted_data(spark):
    """Create sample extracted data DataFrame."""
    data = [("1", 100), ("2", 200)]
    return spark.createDataFrame(data, schema=["id", "amount"])


@pytest.fixture
def sample_transformed_data(spark):
    """Create sample transformed data DataFrame."""
    data = [("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", '[]', "2024-01-01T11:00:00.000Z")]
    schema = StructType([
        StructField("pk", StringType(), nullable=False),
        StructField("sk", StringType(), nullable=False),
        StructField("gsi1_pk", StringType(), nullable=True),
        StructField("gsi1_sk", StringType(), nullable=True),
        StructField("gsi2_pk", StringType(), nullable=True),
        StructField("gsi2_sk", StringType(), nullable=True),
        StructField("productos", StringType(), nullable=True),
        StructField("fecha_actualizacion", StringType(), nullable=True),
    ])
    return spark.createDataFrame(data, schema=schema)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.SnapshotManager")
def test_job_first_run_success(
    mock_snapshot_manager_class,
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test job execution on first run (no tag exists)."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_snapshot_manager = MagicMock()
    mock_snapshot_manager.last_snapshot_tag_exists.return_value = False
    mock_snapshot_manager_class.return_value = mock_snapshot_manager

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "OK"
    assert "completed successfully" in result.message

    mock_snapshot_manager.last_snapshot_tag_exists.assert_called_once_with("portafolios_dynamodb_last_snapshot")
    mock_snapshot_manager.has_changed.assert_not_called()
    mock_extract.assert_called_once_with(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=True
    )
    mock_transform.assert_called_once()
    mock_load.assert_called_once()
    assert mock_load.call_args[1]["first_run"] is True
    mock_snapshot_manager.set_last_snapshot_tag.assert_called_once_with("portafolios_dynamodb_last_snapshot")


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.libs.iceberg.cdc_with_tagging_strategy.ChangelogManager")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.SnapshotManager")
def test_job_subsequent_run_with_changes(
    mock_snapshot_manager_class,
    mock_changelog_manager_class,
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test job execution on subsequent run with changes detected."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_snapshot_manager = MagicMock()
    mock_snapshot_manager.last_snapshot_tag_exists.return_value = True
    mock_snapshot_manager.has_changed.return_value = True
    mock_snapshot_manager_class.return_value = mock_snapshot_manager

    mock_changelog_manager = MagicMock()
    mock_changelog_manager_class.return_value = mock_changelog_manager

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "OK"
    assert "completed successfully" in result.message

    mock_snapshot_manager.has_changed.assert_called_once_with("portafolios_dynamodb_last_snapshot")
    mock_extract.assert_called_once_with(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=False
    )
    mock_load.assert_called_once()
    assert mock_load.call_args[1]["first_run"] is False
    mock_snapshot_manager.set_last_snapshot_tag.assert_called_once_with("portafolios_dynamodb_last_snapshot")


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.SnapshotManager")
def test_job_subsequent_run_no_changes(
    mock_snapshot_manager_class,
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test job execution on subsequent run with no changes detected."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_snapshot_manager = MagicMock()
    mock_snapshot_manager.last_snapshot_tag_exists.return_value = True
    mock_snapshot_manager.has_changed.return_value = False
    mock_snapshot_manager_class.return_value = mock_snapshot_manager

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "OK"
    assert "No changes detected" in result.message

    mock_snapshot_manager.has_changed.assert_called_once()
    mock_extract.assert_not_called()
    mock_transform.assert_not_called()
    mock_load.assert_not_called()


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.restart_table")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.SnapshotManager")
def test_job_with_restart_flag(
    mock_snapshot_manager_class,
    mock_extract,
    mock_transform,
    mock_load,
    mock_restart_table,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test job execution with restart flag set."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_vars_resource.vars.restart.get = MagicMock(return_value=True)
    mock_restart_table.return_value = True

    mock_snapshot_manager = MagicMock()
    mock_snapshot_manager.last_snapshot_tag_exists.return_value = False
    mock_snapshot_manager_class.return_value = mock_snapshot_manager

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "OK"

    mock_restart_table.assert_called_once_with("test-output-table", "us-east-1")
    mock_extract.assert_called_once()


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.restart_table")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.SnapshotManager")
def test_job_restart_failure_returns_failed(
    mock_snapshot_manager_class,
    mock_extract,
    mock_transform,
    mock_load,
    mock_restart_table,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test job returns FAILED status if restart fails."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_vars_resource.vars.restart.get = MagicMock(return_value=True)
    mock_restart_table.return_value = False

    mock_snapshot_manager = MagicMock()
    mock_snapshot_manager.last_snapshot_tag_exists.return_value = False
    mock_snapshot_manager_class.return_value = mock_snapshot_manager

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "ERROR"
    assert "Failed to restart DynamoDB table" in result.message

    mock_restart_table.assert_called_once()
    mock_extract.assert_not_called()


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.SnapshotManager")
def test_job_logging(
    mock_snapshot_manager_class,
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test that job logs correctly at each step."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_snapshot_manager = MagicMock()
    mock_snapshot_manager.last_snapshot_tag_exists.return_value = False
    mock_snapshot_manager_class.return_value = mock_snapshot_manager

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    portafolios_dynamodb_job(spark, mock_vars_resource)

    assert mock_logger.info.call_count >= 6
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Starting portafolios dynamodb processing job" in msg for msg in log_calls)
    assert any("First run detection completed" in msg for msg in log_calls)
    assert any("Starting data extraction" in msg for msg in log_calls)
    assert any("Starting data transformation" in msg for msg in log_calls)
    assert any("Starting data load" in msg for msg in log_calls)
    assert any("Portafolios dynamodb processing job completed successfully" in msg for msg in log_calls)
