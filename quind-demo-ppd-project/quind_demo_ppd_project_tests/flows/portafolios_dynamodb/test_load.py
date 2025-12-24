import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, StructType as SparkStructType
from pyspark.testing.utils import assertDataFrameEqual

from quind_demo_ppd_project.flows.portafolios_dynamodb.load import load


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.output.table_name = "test-portafolios-table"
    mock_vars.vars.output.region = "us-east-1"
    mock_vars.vars.output.item_size_limit = 389120
    return mock_vars


@pytest.fixture
def sample_transformed_data(spark):
    """Create sample transformed data for testing."""
    productos_schema = ArrayType(
        StructType([
            StructField("cod_material", StringType(), True),
            StructField("des_material", StringType(), True),
        ])
    )

    data = [
        (
            "TXN001",
            "ORG1#CANAL1#VEND1",
            "ORG1",
            "CANAL1#TXN001",
            "VEND1",
            "TXN001",
            [{"cod_material": "MAT001", "des_material": "Product1"}],
            "2024-01-01T10:00:00.000Z",
        ),
        (
            "TXN002",
            "ORG2#CANAL2#VEND2",
            "ORG2",
            "CANAL2#TXN002",
            "VEND2",
            "TXN002",
            [{"cod_material": "MAT002", "des_material": "Product2"}],
            "2024-01-02T10:00:00.000Z",
        ),
    ]

    return spark.createDataFrame(
        data,
        schema=[
            "pk",
            "sk",
            "gsi1_pk",
            "gsi1_sk",
            "gsi2_pk",
            "gsi2_sk",
            "productos",
            "fecha_actualizacion",
        ],
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
def test_load_first_run(mock_get_logger, mock_loader_class, spark, mock_vars_resource, sample_transformed_data):
    """Test load with first_run=True (overwrite mode)."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    job_id = "test_job_123"

    load(
        job_id=job_id,
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data,
        first_run=True,
    )

    mock_loader_class.assert_called_once_with(
        job_id=job_id,
        vars_instance=mock_vars_resource,
    )

    mock_loader_instance.load.assert_called_once()
    call_args = mock_loader_instance.load.call_args

    assert "dataframes" in call_args.kwargs
    assert "data_to_write" in call_args.kwargs["dataframes"]
    assert "data_to_delete" in call_args.kwargs["dataframes"]
    assert call_args.kwargs["first_run"] is True
    assert call_args.kwargs["write_handler"] == "portfolio_writer"
    assert call_args.kwargs["delete_handler"] == "delete"

    write_df = call_args.kwargs["dataframes"]["data_to_write"]
    delete_df = call_args.kwargs["dataframes"]["data_to_delete"]

    assertDataFrameEqual(write_df, sample_transformed_data)
    assert delete_df.count() == 0


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
def test_load_not_first_run(mock_get_logger, mock_loader_class, spark, mock_vars_resource, sample_transformed_data):
    """Test load with first_run=False (merge mode)."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    job_id = "test_job_123"

    load(
        job_id=job_id,
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data,
        first_run=False,
    )

    call_args = mock_loader_instance.load.call_args
    assert call_args.kwargs["first_run"] is False


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
def test_load_default_first_run(mock_get_logger, mock_loader_class, spark, mock_vars_resource, sample_transformed_data):
    """Test load with default first_run=True."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    job_id = "test_job_123"

    load(
        job_id=job_id,
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data,
    )

    call_args = mock_loader_instance.load.call_args
    assert call_args.kwargs["first_run"] is True


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.load.get_logger")
def test_load_empty_dataframe(mock_get_logger, mock_loader_class, spark, mock_vars_resource):
    """Test load with empty DataFrame."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_loader_instance = MagicMock()
    mock_loader_class.return_value = mock_loader_instance

    productos_schema = ArrayType(
        StructType([
            StructField("cod_material", StringType(), True),
            StructField("des_material", StringType(), True),
        ])
    )

    empty_schema = StructType([
        StructField("pk", StringType(), True),
        StructField("sk", StringType(), True),
        StructField("gsi1_pk", StringType(), True),
        StructField("gsi1_sk", StringType(), True),
        StructField("gsi2_pk", StringType(), True),
        StructField("gsi2_sk", StringType(), True),
        StructField("productos", productos_schema, True),
        StructField("fecha_actualizacion", StringType(), True),
    ])
    empty_df = spark.createDataFrame([], schema=empty_schema)

    job_id = "test_job_123"

    load(
        job_id=job_id,
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=empty_df,
    )

    call_args = mock_loader_instance.load.call_args
    write_df = call_args.kwargs["dataframes"]["data_to_write"]
    assert write_df.count() == 0
