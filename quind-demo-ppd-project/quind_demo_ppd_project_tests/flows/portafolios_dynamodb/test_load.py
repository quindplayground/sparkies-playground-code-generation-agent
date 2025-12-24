import pytest
from unittest.mock import MagicMock, Mock, patch, PropertyMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
import gzip
import json

from quind_demo_ppd_project.flows.portafolios_dynamodb.load import load
from quind_demo_ppd_project.libs.resources import VarsResource


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource with DynamoDB configuration."""
    mock_vars = MagicMock(spec=VarsResource)
    mock_vars.vars.output.table_name = "portafolio-cliente-test"
    mock_vars.vars.output.region = "us-east-1"
    mock_vars.vars.output.item_size_limit = 400000
    mock_vars.vars.output.table_id = "test.catalog.database.portafolios_output"
    return mock_vars


def create_transformed_dataframe(spark, data):
    """Helper to create transformed DataFrame with expected schema."""
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


def test_load_with_valid_data(spark, mock_vars_resource):
    """Test load with valid transformed data."""
    productos_data = [{"cod_material": "MAT1", "amount": 1000}]
    productos_json = json.dumps(productos_data)
    productos_binary = gzip.compress(productos_json.encode("utf-8"))

    transformed_data = create_transformed_dataframe(
        spark,
        [
            (
                "TXN1",
                "ORG1#CANAL1#VEND1",
                "ORG1",
                "CANAL1#TXN1",
                "VEND1",
                "TXN1",
                productos_binary,
                "2024-01-01T12:00:00.000Z",
            ),
        ],
    )

    job_id = "test_job_123"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader"
    ) as mock_loader_class:
        mock_loader_instance = MagicMock()
        mock_loader_class.return_value = mock_loader_instance

        load(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars_resource,
            transformed_data=transformed_data,
        )

        mock_loader_class.assert_called_once_with(
            job_id=job_id, vars_instance=mock_vars_resource
        )
        mock_loader_instance.load.assert_called_once()
        call_args = mock_loader_instance.load.call_args
        assert "dataframes" in call_args.kwargs
        assert "first_run" in call_args.kwargs
        assert call_args.kwargs["first_run"] is False
        assert call_args.kwargs["write_handler"] == "portfolio_writer"
        assert call_args.kwargs["delete_handler"] == "delete"


def test_load_with_first_run_flag(spark, mock_vars_resource):
    """Test load with first_run=True."""
    productos_data = [{"cod_material": "MAT1", "amount": 1000}]
    productos_json = json.dumps(productos_data)
    productos_binary = gzip.compress(productos_json.encode("utf-8"))

    transformed_data = create_transformed_dataframe(
        spark,
        [
            (
                "TXN1",
                "ORG1#CANAL1#VEND1",
                "ORG1",
                "CANAL1#TXN1",
                "VEND1",
                "TXN1",
                productos_binary,
                "2024-01-01T12:00:00.000Z",
            ),
        ],
    )

    job_id = "test_job_456"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader"
    ) as mock_loader_class:
        mock_loader_instance = MagicMock()
        mock_loader_class.return_value = mock_loader_instance

        load(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars_resource,
            transformed_data=transformed_data,
            first_run=True,
        )

        call_args = mock_loader_instance.load.call_args
        assert call_args.kwargs["first_run"] is True


def test_load_with_empty_dataframe(spark, mock_vars_resource):
    """Test load with empty transformed DataFrame."""
    empty_df = spark.createDataFrame(
        [],
        schema=StructType(
            [
                StructField("pk", StringType(), True),
                StructField("sk", StringType(), True),
                StructField("gsi1_pk", StringType(), True),
                StructField("gsi1_sk", StringType(), True),
                StructField("gsi2_pk", StringType(), True),
                StructField("gsi2_sk", StringType(), True),
                StructField("productos", BinaryType(), True),
                StructField("fecha_actualizacion", StringType(), True),
            ]
        ),
    )

    job_id = "test_job_empty"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader"
    ) as mock_loader_class:
        mock_loader_instance = MagicMock()
        mock_loader_class.return_value = mock_loader_instance

        load(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars_resource,
            transformed_data=empty_df,
        )

        mock_loader_instance.load.assert_called_once()
        call_args = mock_loader_instance.load.call_args
        assert call_args.kwargs["dataframes"]["data_to_write"].count() == 0
        assert call_args.kwargs["dataframes"]["data_to_delete"].count() == 0


def test_load_missing_table_name_config(spark):
    """Test load raises KeyError when table_name is missing."""
    mock_vars = MagicMock(spec=VarsResource)
    mock_vars.vars.output = Mock(spec=[])
    # Don't set table_name - accessing it will raise AttributeError

    productos_data = [{"cod_material": "MAT1", "amount": 1000}]
    productos_json = json.dumps(productos_data)
    productos_binary = gzip.compress(productos_json.encode("utf-8"))

    transformed_data = create_transformed_dataframe(
        spark,
        [
            (
                "TXN1",
                "ORG1#CANAL1#VEND1",
                "ORG1",
                "CANAL1#TXN1",
                "VEND1",
                "TXN1",
                productos_binary,
                "2024-01-01T12:00:00.000Z",
            ),
        ],
    )

    job_id = "test_job_error"

    with pytest.raises(KeyError, match="DynamoDB table_name not configured"):
        load(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars,
            transformed_data=transformed_data,
        )


def test_load_missing_region_config(spark):
    """Test load raises KeyError when region is missing."""
    mock_vars = MagicMock(spec=VarsResource)
    mock_vars.vars.output = Mock(spec=['table_name'])
    mock_vars.vars.output.table_name = "portafolio-cliente-test"
    # Don't set region - accessing it will raise AttributeError

    productos_data = [{"cod_material": "MAT1", "amount": 1000}]
    productos_json = json.dumps(productos_data)
    productos_binary = gzip.compress(productos_json.encode("utf-8"))

    transformed_data = create_transformed_dataframe(
        spark,
        [
            (
                "TXN1",
                "ORG1#CANAL1#VEND1",
                "ORG1",
                "CANAL1#TXN1",
                "VEND1",
                "TXN1",
                productos_binary,
                "2024-01-01T12:00:00.000Z",
            ),
        ],
    )

    job_id = "test_job_error"

    with pytest.raises(KeyError, match="AWS region not configured"):
        load(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars,
            transformed_data=transformed_data,
        )


def test_load_uses_default_item_size_limit(spark):
    """Test load uses default item_size_limit when not configured."""
    mock_vars = MagicMock(spec=VarsResource)
    mock_vars.vars.output.table_name = "portafolio-cliente-test"
    mock_vars.vars.output.region = "us-east-1"
    mock_vars.vars.output.item_size_limit = 400000
    mock_vars.vars.get = MagicMock(return_value=None)

    productos_data = [{"cod_material": "MAT1", "amount": 1000}]
    productos_json = json.dumps(productos_data)
    productos_binary = gzip.compress(productos_json.encode("utf-8"))

    transformed_data = create_transformed_dataframe(
        spark,
        [
            (
                "TXN1",
                "ORG1#CANAL1#VEND1",
                "ORG1",
                "CANAL1#TXN1",
                "VEND1",
                "TXN1",
                productos_binary,
                "2024-01-01T12:00:00.000Z",
            ),
        ],
    )

    job_id = "test_job_default"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.load.DynamoDBLoader"
    ) as mock_loader_class:
        mock_loader_instance = MagicMock()
        mock_loader_class.return_value = mock_loader_instance

        load(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars,
            transformed_data=transformed_data,
        )

        mock_loader_instance.load.assert_called_once()
