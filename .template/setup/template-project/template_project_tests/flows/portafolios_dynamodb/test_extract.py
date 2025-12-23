import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

from template_project.flows.portafolios_dynamodb.extract import extract
from template_project.libs.iceberg.cdc_with_tagging_strategy import ChangelogManager


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource for testing."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test_catalog.test_schema.test_table"
    mock_vars.vars.component_name = "portafolios_dynamodb"
    mock_vars.vars.get = MagicMock(side_effect=lambda key, default=None: {
        "last_snapshot_tag_name": "portafolios_dynamodb_last_snapshot",
    }.get(key, default))
    return mock_vars


@patch("template_project.flows.portafolios_dynamodb.extract.get_logger")
@patch.object(SparkSession, "table")
def test_extract_first_run_full_table(
    mock_spark_table,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test extraction on first run (extracts complete table)."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    expected_data = [("1", 100, True), ("2", 200, False)]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "amount", "active"]
    )
    mock_spark_table.return_value = expected_df

    result = extract(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=True
    )

    assertDataFrameEqual(result, expected_df)
    mock_spark_table.assert_called_once_with("test_catalog.test_schema.test_table")


@patch("template_project.flows.portafolios_dynamodb.extract.get_logger")
@patch("template_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_incremental_run_changelog(
    mock_changelog_manager_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test extraction on incremental run (extracts changelog)."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    expected_data = [("1", 100, True), ("2", 200, False)]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "amount", "active"]
    )

    mock_manager_instance = MagicMock()
    mock_manager_instance.get_changelog_table.return_value = expected_df
    mock_changelog_manager_class.return_value = mock_manager_instance

    result = extract(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=False
    )

    assertDataFrameEqual(result, expected_df)
    mock_changelog_manager_class.assert_called_once_with(
        spark=spark,
        table_id="test_catalog.test_schema.test_table",
        tag_name="portafolios_dynamodb_last_snapshot"
    )
    mock_manager_instance.get_changelog_table.assert_called_once_with(changelog_strategy="iceberg_sp")


@patch("template_project.flows.portafolios_dynamodb.extract.get_logger")
@patch.object(SparkSession, "table")
def test_extract_default_first_run(
    mock_spark_table,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test extraction defaults to first_run=True when not specified."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    expected_data = [("1", 100), ("2", 200)]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "amount"]
    )
    mock_spark_table.return_value = expected_df

    result = extract(
        spark=spark,
        vars_instance=mock_vars_resource
    )

    assertDataFrameEqual(result, expected_df)
    mock_spark_table.assert_called_once_with("test_catalog.test_schema.test_table")


@patch("template_project.flows.portafolios_dynamodb.extract.get_logger")
@patch.object(SparkSession, "table")
def test_extract_empty_result(
    mock_spark_table,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test extraction with empty DataFrame result."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    empty_df = spark.createDataFrame([], schema=StructType([
        StructField("id", StringType(), nullable=True),
        StructField("amount", IntegerType(), nullable=True)
    ]))
    mock_spark_table.return_value = empty_df

    result = extract(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=True
    )

    assertDataFrameEqual(result, empty_df)
    assert result.count() == 0


@patch("template_project.flows.portafolios_dynamodb.extract.logger")
@patch.object(SparkSession, "table")
def test_extract_logging_first_run(
    mock_spark_table,
    mock_logger,
    spark,
    mock_vars_resource,
):
    """Test that extraction logs correctly on first run."""
    expected_data = [("1", 100), ("2", 200)]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "amount"]
    )
    mock_spark_table.return_value = expected_df

    extract(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=True
    )

    assert mock_logger.info.call_count >= 2
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Starting data extraction" in msg for msg in log_calls)
    assert any("First run detected" in msg for msg in log_calls)
    assert any("Data extraction completed" in msg for msg in log_calls)


@patch("template_project.flows.portafolios_dynamodb.extract.logger")
@patch("template_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_logging_incremental_run(
    mock_changelog_manager_class,
    mock_logger,
    spark,
    mock_vars_resource,
):
    """Test that extraction logs correctly on incremental run."""
    expected_data = [("1", 100), ("2", 200)]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "amount"]
    )

    mock_manager_instance = MagicMock()
    mock_manager_instance.get_changelog_table.return_value = expected_df
    mock_changelog_manager_class.return_value = mock_manager_instance

    extract(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=False
    )

    assert mock_logger.info.call_count >= 2
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Starting data extraction" in msg for msg in log_calls)
    assert any("Incremental run detected" in msg for msg in log_calls)
    assert any("Data extraction completed" in msg for msg in log_calls)


@patch("template_project.flows.portafolios_dynamodb.extract.get_logger")
@patch("template_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_custom_tag_name(
    mock_changelog_manager_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test extraction with custom tag name from config."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    expected_data = [("1", 100)]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "amount"]
    )

    mock_vars_resource.vars.get = MagicMock(side_effect=lambda key, default=None: {
        "last_snapshot_tag_name": "custom_tag_name",
    }.get(key, default))

    mock_manager_instance = MagicMock()
    mock_manager_instance.get_changelog_table.return_value = expected_df
    mock_changelog_manager_class.return_value = mock_manager_instance

    result = extract(
        spark=spark,
        vars_instance=mock_vars_resource,
        first_run=False
    )

    assertDataFrameEqual(result, expected_df)
    mock_changelog_manager_class.assert_called_once_with(
        spark=spark,
        table_id="test_catalog.test_schema.test_table",
        tag_name="custom_tag_name"
    )
