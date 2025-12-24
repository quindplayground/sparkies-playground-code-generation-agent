import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from quind_demo_ppd_project.flows.portafolios_dynamodb.transform import transform


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test.catalog.portafolios"
    mock_vars.vars.output.table_id = "test.output.portafolios"
    return mock_vars


def test_transform_deduplication(spark, mock_vars_resource):
    """Test transformation with deduplication."""
    input_data = [
        ("1", "Portfolio A", 1000.50),
        ("2", "Portfolio B", 3000.75),
        ("1", "Portfolio A", 1000.50),
        ("3", "Portfolio C", 500.25),
    ]
    input_df = spark.createDataFrame(
        input_data,
        schema=["id", "nombre", "valor"],
    )

    result = transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=input_df,
    )

    expected_data = [
        ("1", "Portfolio A", 1000.50),
        ("2", "Portfolio B", 3000.75),
        ("3", "Portfolio C", 500.25),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "nombre", "valor"],
    )

    result_without_timestamp = result.select("id", "nombre", "valor")
    assertDataFrameEqual(result_without_timestamp, expected_df, checkRowOrder=False)
    assert result.count() == 3


def test_transform_adds_timestamp(spark, mock_vars_resource):
    """Test transformation adds current_timestamp_dwh column."""
    input_data = [
        ("1", "Portfolio A", 1000.50),
        ("2", "Portfolio B", 3000.75),
    ]
    input_df = spark.createDataFrame(
        input_data,
        schema=["id", "nombre", "valor"],
    )

    result = transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=input_df,
    )

    assert "current_timestamp_dwh" in result.columns
    assert result.select("current_timestamp_dwh").first()[0] is not None


def test_transform_empty_dataframe(spark, mock_vars_resource):
    """Test transformation with empty DataFrame."""
    empty_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("nombre", StringType(), True),
            StructField("valor", DoubleType(), True),
        ]
    )
    input_df = spark.createDataFrame([], schema=empty_schema)

    result = transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=input_df,
    )

    assert result.count() == 0
    assert "current_timestamp_dwh" in result.columns


def test_transform_preserves_all_columns(spark, mock_vars_resource):
    """Test transformation preserves all original columns."""
    input_data = [
        ("1", "Portfolio A", 1000.50, "ACTIVE"),
        ("2", "Portfolio B", 3000.75, "INACTIVE"),
    ]
    input_df = spark.createDataFrame(
        input_data,
        schema=["id", "nombre", "valor", "estado"],
    )

    result = transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=input_df,
    )

    original_columns = set(input_df.columns)
    result_columns = set(result.columns) - {"current_timestamp_dwh"}
    assert original_columns == result_columns
