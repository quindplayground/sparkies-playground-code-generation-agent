import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, BinaryType
from datetime import datetime

from quind_demo_ppd_project.flows.portafolios_dynamodb.job import portafolios_dynamodb_job
from quind_demo_ppd_project.libs.resources import VarsResource
from quind_demo_ppd_project.libs.runner.types import Status


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock(spec=VarsResource)
    mock_vars.vars.input.table_id = "test.catalog.database.portafolios"
    mock_vars.vars.output.table_id = "test.catalog.database.portafolios_output"
    mock_vars.vars.get.return_value = "portafolios_dynamodb"
    return mock_vars


def test_job_full_pipeline_success(spark, mock_vars_resource):
    """Test full ETL pipeline execution."""
    input_data = [
        (
            "TXN1",
            "ORG1",
            "CANAL1",
            "VEND1",
            True,
            True,
            "MAT1",
            datetime(2024, 1, 1, 12, 0, 0),
        ),
    ]
    extracted_df = spark.createDataFrame(
        input_data,
        schema=[
            "cod_transaccional",
            "cod_org_vent",
            "cod_canal",
            "cod_vendedor",
            "ind_cliente_activo",
            "ind_material_activo",
            "cod_material",
            "fec_actualizacion_dl",
        ],
    )

    transformed_df = spark.createDataFrame(
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

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract"
    ) as mock_extract, patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform"
    ) as mock_transform, patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.load"
    ) as mock_load:
        mock_extract.return_value = extracted_df
        mock_transform.return_value = transformed_df
        mock_load.return_value = None

        result = portafolios_dynamodb_job(spark, mock_vars_resource)

        assert isinstance(result, Status)
        assert result.status_value == "OK"
        assert "completed successfully" in result.message.lower()

        mock_extract.assert_called_once_with(
            spark=spark, vars_instance=mock_vars_resource
        )
        mock_transform.assert_called_once()
        assert mock_transform.call_args.kwargs["job_id"] is not None
        assert mock_transform.call_args.kwargs["spark"] == spark
        assert mock_transform.call_args.kwargs["vars_instance"] == mock_vars_resource
        assert mock_transform.call_args.kwargs["extracted_data"] == extracted_df

        mock_load.assert_called_once()
        assert mock_load.call_args.kwargs["job_id"] is not None
        assert mock_load.call_args.kwargs["spark"] == spark
        assert mock_load.call_args.kwargs["vars_instance"] == mock_vars_resource
        assert mock_load.call_args.kwargs["transformed_data"] == transformed_df


def test_job_with_empty_extraction(spark, mock_vars_resource):
    """Test job with empty extraction result."""
    empty_df = spark.createDataFrame(
        [],
        schema=StructType(
            [
                StructField("cod_transaccional", StringType(), True),
                StructField("cod_org_vent", StringType(), True),
                StructField("cod_canal", StringType(), True),
                StructField("cod_vendedor", StringType(), True),
                StructField("ind_cliente_activo", BooleanType(), True),
                StructField("ind_material_activo", BooleanType(), True),
            ]
        ),
    )

    transformed_df = spark.createDataFrame(
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

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract"
    ) as mock_extract, patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform"
    ) as mock_transform, patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.load"
    ) as mock_load:
        mock_extract.return_value = empty_df
        mock_transform.return_value = transformed_df
        mock_load.return_value = None

        result = portafolios_dynamodb_job(spark, mock_vars_resource)

        assert isinstance(result, Status)
        assert result.status_value == "OK"
        mock_extract.assert_called_once()
        mock_transform.assert_called_once()
        mock_load.assert_called_once()


def test_job_generates_unique_job_id(spark, mock_vars_resource):
    """Test that job generates unique job_id for each execution."""
    input_data = [
        (
            "TXN1",
            "ORG1",
            "CANAL1",
            "VEND1",
            True,
            True,
            "MAT1",
            datetime(2024, 1, 1, 12, 0, 0),
        ),
    ]
    extracted_df = spark.createDataFrame(
        input_data,
        schema=[
            "cod_transaccional",
            "cod_org_vent",
            "cod_canal",
            "cod_vendedor",
            "ind_cliente_activo",
            "ind_material_activo",
            "cod_material",
            "fec_actualizacion_dl",
        ],
    )

    transformed_df = spark.createDataFrame(
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

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract"
    ) as mock_extract, patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform"
    ) as mock_transform, patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.job.load"
    ) as mock_load:
        mock_extract.return_value = extracted_df
        mock_transform.return_value = transformed_df
        mock_load.return_value = None

        result1 = portafolios_dynamodb_job(spark, mock_vars_resource)
        result2 = portafolios_dynamodb_job(spark, mock_vars_resource)

        assert isinstance(result1, Status)
        assert isinstance(result2, Status)
        assert result1.status_value == "OK"
        assert result2.status_value == "OK"

        job_id1 = mock_transform.call_args_list[0].kwargs["job_id"]
        job_id2 = mock_transform.call_args_list[1].kwargs["job_id"]
        assert job_id1 != job_id2
        assert job_id1.startswith("portafolios_dynamodb_")
        assert job_id2.startswith("portafolios_dynamodb_")
