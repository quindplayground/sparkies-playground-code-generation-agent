import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType

from quind_demo_ppd_project.flows.portafolios_dynamodb.load import load


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource for testing."""
    mock_vars = MagicMock()
    mock_vars.vars.output.table_name = "test-table"
    mock_vars.vars.output.region = "us-east-1"
    mock_vars.vars.output.item_size_limit = 389120
    return mock_vars


@pytest.fixture
def sample_transformed_data(spark):
    """Create sample transformed data DataFrame."""
    data = [
        ("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", '[]', "2024-01-01T11:00:00.000Z"),
        ("CLI002", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI002", "VEN001", "CLI002", '[]', "2024-01-01T12:00:00.000Z"),
    ]
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


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_first_run(
    mock_loader_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_transformed_data,
):
    """Test load with first_run=True (write only)."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data,
        first_run=True
    )

    mock_loader_class.assert_called_once_with(
        job_id="test_job_123",
        vars_instance=mock_vars_resource
    )

    mock_loader_instance.load.assert_called_once()
    call_args = mock_loader_instance.load.call_args
    assert call_args[1]["first_run"] is True
    assert call_args[1]["write_handler"] == "portfolio_writer"
    assert call_args[1]["delete_handler"] == "delete"

    dataframes = call_args[1]["dataframes"]
    assert "data_to_write" in dataframes
    assert "data_to_delete" in dataframes
    assertDataFrameEqual(dataframes["data_to_write"], sample_transformed_data)
    assert dataframes["data_to_delete"].count() == 0


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_not_first_run(
    mock_loader_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_transformed_data,
):
    """Test load with first_run=False (write and delete)."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data,
        first_run=False
    )

    call_args = mock_loader_instance.load.call_args
    assert call_args[1]["first_run"] is False

    dataframes = call_args[1]["dataframes"]
    assert "data_to_write" in dataframes
    assert "data_to_delete" in dataframes
    assertDataFrameEqual(dataframes["data_to_write"], sample_transformed_data)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_default_first_run(
    mock_loader_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_transformed_data,
):
    """Test load with default first_run=True when not specified."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data
    )

    call_args = mock_loader_instance.load.call_args
    assert call_args[1]["first_run"] is True


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_empty_dataframe(
    mock_loader_class,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test load with empty transformed DataFrame."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    empty_df = spark.createDataFrame([], schema=StructType([
        StructField("pk", StringType(), nullable=False),
        StructField("sk", StringType(), nullable=False),
        StructField("productos", StringType(), nullable=True),
        StructField("fecha_actualizacion", StringType(), nullable=True),
    ]))

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=empty_df
    )

    call_args = mock_loader_instance.load.call_args
    dataframes = call_args[1]["dataframes"]
    assert dataframes["data_to_write"].count() == 0
    assert dataframes["data_to_delete"].count() == 0


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
def test_load_missing_region_raises_error(
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_transformed_data,
):
    """Test load raises ValueError when region is not specified in config."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    delattr(mock_vars_resource.vars.output, "region")
    mock_vars_resource.vars.output.table_name = "test-table"

    with pytest.raises(ValueError, match="DynamoDB region is required"):
        load(
            job_id="test_job_123",
            spark=spark,
            vars_instance=mock_vars_resource,
            transformed_data=sample_transformed_data
        )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
def test_load_logging(
    mock_loader_class,
    mock_logger,
    spark,
    mock_vars_resource,
    sample_transformed_data,
):
    """Test that load logs correctly."""

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    load(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data,
        first_run=True
    )

    assert mock_logger.info.call_count == 2
    first_call = mock_logger.info.call_args_list[0]
    assert "Loading data to DynamoDB" in first_call[0][0]
    second_call = mock_logger.info.call_args_list[1]
    assert "Data loaded to DynamoDB" in second_call[0][0]
