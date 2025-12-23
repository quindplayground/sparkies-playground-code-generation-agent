import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from quind_demo_ppd_project.flows.portafolios_dynamodb.extract import extract
from quind_demo_ppd_project.libs.iceberg.cdc_with_tagging_strategy import ChangelogManager


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource for testing."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test_catalog.test_schema.test_table"
    mock_vars.vars.get = MagicMock(side_effect=lambda key, default=None: {
        "last_snapshot_tag_name": "portafolios_dynamodb_last_snapshot",
    }.get(key, default))
    return mock_vars


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_basic(
    mock_changelog_manager_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test basic extraction using ChangelogManager."""
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
    )

    assertDataFrameEqual(result, expected_df)
    mock_changelog_manager_class.assert_called_once_with(
        spark=spark,
        table_id="test_catalog.test_schema.test_table",
        tag_name="portafolios_dynamodb_last_snapshot"
    )
    mock_manager_instance.get_changelog_table.assert_called_once_with(
        changelog_strategy="iceberg_sp"
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_empty_result(
    mock_changelog_manager_class,
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

    mock_manager_instance = MagicMock()
    mock_manager_instance.get_changelog_table.return_value = empty_df
    mock_changelog_manager_class.return_value = mock_manager_instance

    result = extract(
        spark=spark,
        vars_instance=mock_vars_resource,
    )

    assertDataFrameEqual(result, empty_df)
    assert result.count() == 0


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_logging(
    mock_changelog_manager_class,
    mock_logger,
    spark,
    mock_vars_resource,
):
    """Test that extraction logs correctly."""
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
    )

    assert mock_logger.info.call_count >= 2
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Extracting data from Iceberg source table using CDC" in msg for msg in log_calls)
    assert any("Data extraction completed" in msg for msg in log_calls)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.ChangelogManager")
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
    )

    assertDataFrameEqual(result, expected_df)
    mock_changelog_manager_class.assert_called_once_with(
        spark=spark,
        table_id="test_catalog.test_schema.test_table",
        tag_name="custom_tag_name"
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_with_default_tag_name(
    mock_changelog_manager_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test extraction uses default tag name when not specified in config."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    expected_data = [("1", 100), ("2", 200)]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "amount"]
    )

    mock_vars_resource.vars.get = MagicMock(return_value=None)

    mock_manager_instance = MagicMock()
    mock_manager_instance.get_changelog_table.return_value = expected_df
    mock_changelog_manager_class.return_value = mock_manager_instance

    result = extract(
        spark=spark,
        vars_instance=mock_vars_resource,
    )

    assertDataFrameEqual(result, expected_df)
    mock_changelog_manager_class.assert_called_once_with(
        spark=spark,
        table_id="test_catalog.test_schema.test_table",
        tag_name=None
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.extract.ChangelogManager")
def test_extract_always_uses_iceberg_sp_strategy(
    mock_changelog_manager_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test that extraction always uses iceberg_sp changelog strategy."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    expected_data = [("1", 100)]
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
    )

    mock_manager_instance.get_changelog_table.assert_called_once_with(
        changelog_strategy="iceberg_sp"
    )
