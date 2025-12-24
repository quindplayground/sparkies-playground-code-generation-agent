import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from quind_demo_ppd_project.flows.portafolios_dynamodb.load import load


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.output.table_name = "test-portafolios-table"
    mock_vars.vars.output.region = "us-east-1"
    mock_vars.vars.output.get.return_value = 400000
    return mock_vars


@pytest.fixture
def transformed_data(spark):
    """Create sample transformed DataFrame."""
    data = [
        ("1", "Portfolio A", 1000.50),
        ("2", "Portfolio B", 3000.75),
    ]
    return spark.createDataFrame(
        data,
        schema=["id", "nombre", "valor"],
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_first_run(mock_loader_class, spark, mock_vars_resource, transformed_data):
    """Test load with first_run=True (default)."""
    mock_loader = MagicMock()
    mock_loader_class.return_value = mock_loader

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=transformed_data,
    )

    mock_loader_class.assert_called_once_with(
        job_id="test_job_123",
        vars_instance=mock_vars_resource,
    )

    mock_loader.load.assert_called_once()
    call_args = mock_loader.load.call_args

    assert call_args.kwargs["first_run"] is True
    assert call_args.kwargs["write_handler"] == "portfolio_writer"
    assert call_args.kwargs["delete_handler"] == "delete"

    dataframes = call_args.kwargs["dataframes"]
    assert "data_to_write" in dataframes
    assert "data_to_delete" in dataframes
    assert dataframes["data_to_write"] == transformed_data
    assert dataframes["data_to_delete"].count() == 0


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_incremental(mock_loader_class, spark, mock_vars_resource, transformed_data):
    """Test load with first_run=False (incremental)."""
    mock_loader = MagicMock()
    mock_loader_class.return_value = mock_loader

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=transformed_data,
        first_run=False,
    )

    call_args = mock_loader.load.call_args
    assert call_args.kwargs["first_run"] is False


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_empty_dataframe(mock_loader_class, spark, mock_vars_resource):
    """Test load with empty DataFrame."""
    empty_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("nombre", StringType(), True),
            StructField("valor", DoubleType(), True),
        ]
    )
    empty_df = spark.createDataFrame([], schema=empty_schema)

    mock_loader = MagicMock()
    mock_loader_class.return_value = mock_loader

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=empty_df,
    )

    call_args = mock_loader.load.call_args
    dataframes = call_args.kwargs["dataframes"]
    assert dataframes["data_to_write"].count() == 0
    assert dataframes["data_to_delete"].count() == 0


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_uses_custom_item_size_limit(mock_loader_class, spark, mock_vars_resource, transformed_data):
    """Test load uses custom item_size_limit from vars."""
    mock_vars_resource.vars.output.get.return_value = 500000

    mock_loader = MagicMock()
    mock_loader_class.return_value = mock_loader

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=transformed_data,
    )

    mock_loader.load.assert_called_once()
