import pytest
import time
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from pyspark.testing.utils import assertDataFrameEqual

from quind_demo_ppd_project.flows.portafolios_dynamodb.job import portafolios_dynamodb_job
from quind_demo_ppd_project.libs.runner.types import Status


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test.catalog.database.portafolios"
    mock_vars.vars.output.table_id = "test.output.table"
    mock_vars.vars.output.table_name = "test-portafolios-table"
    mock_vars.vars.output.region = "us-east-1"
    mock_vars.vars.output.item_size_limit = 389120
    mock_vars.vars.get.return_value = "portafolios_dynamodb"
    return mock_vars


@pytest.fixture
def sample_extracted_data(spark):
    """Create sample extracted data for testing."""
    data = [
        (
            "TXN001",
            "ORG1",
            "CANAL1",
            "VEND1",
            True,
            True,
            "2024-01-01 10:00:00",
            "MAT001",
            "Product1",
        ),
        (
            "TXN002",
            "ORG2",
            "CANAL2",
            "VEND2",
            True,
            True,
            "2024-01-02 10:00:00",
            "MAT002",
            "Product2",
        ),
    ]
    return spark.createDataFrame(
        data,
        schema=[
            "cod_transaccional",
            "cod_org_vent",
            "cod_canal",
            "cod_vendedor",
            "ind_cliente_activo",
            "ind_material_activo",
            "fec_actualizacion_dl",
            "cod_material",
            "des_material",
        ],
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
def test_job_full_pipeline(
    mock_get_logger,
    mock_extract,
    mock_transform,
    mock_load,
    spark,
    mock_vars_resource,
    sample_extracted_data,
):
    """Test full ETL pipeline execution."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_extracted_df = sample_extracted_data
    mock_transformed_df = spark.createDataFrame(
        [("TXN001", "ORG1#CANAL1#VEND1", [{"cod_material": "MAT001"}], "2024-01-01T10:00:00.000Z")],
        schema=["pk", "sk", "productos", "fecha_actualizacion"],
    )

    mock_extract.return_value = mock_extracted_df
    mock_transform.return_value = mock_transformed_df

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "OK"
    assert "completed successfully" in result.message.lower()

    mock_extract.assert_called_once_with(
        spark=spark,
        vars_instance=mock_vars_resource,
    )

    extract_call = mock_transform.call_args
    assert extract_call.kwargs["job_id"].startswith("portafolios_dynamodb_")
    assert extract_call.kwargs["spark"] == spark
    assert extract_call.kwargs["vars_instance"] == mock_vars_resource
    assertDataFrameEqual(extract_call.kwargs["extracted_data"], mock_extracted_df)

    load_call = mock_load.call_args
    assert load_call.kwargs["job_id"].startswith("portafolios_dynamodb_")
    assert load_call.kwargs["spark"] == spark
    assert load_call.kwargs["vars_instance"] == mock_vars_resource
    assertDataFrameEqual(load_call.kwargs["transformed_data"], mock_transformed_df)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
def test_job_logging(
    mock_get_logger,
    mock_extract,
    mock_transform,
    mock_load,
    spark,
    mock_vars_resource,
    sample_extracted_data,
):
    """Test that job logs all steps correctly."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_extracted_df = sample_extracted_data
    mock_transformed_df = spark.createDataFrame(
        [("TXN001", "ORG1#CANAL1#VEND1", [{"cod_material": "MAT001"}], "2024-01-01T10:00:00.000Z")],
        schema=["pk", "sk", "productos", "fecha_actualizacion"],
    )

    mock_extract.return_value = mock_extracted_df
    mock_transform.return_value = mock_transformed_df

    portafolios_dynamodb_job(spark, mock_vars_resource)

    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]

    assert any("Starting portafolios dynamodb processing job" in msg for msg in log_calls)
    assert any("Starting data extraction" in msg for call in mock_logger.info.call_args_list for msg in [call[0][0]])
    assert any("Data extraction completed" in msg for call in mock_logger.info.call_args_list for msg in [call[0][0]])
    assert any("Starting data transformation" in msg for call in mock_logger.info.call_args_list for msg in [call[0][0]])
    assert any("Data transformation completed" in msg for call in mock_logger.info.call_args_list for msg in [call[0][0]])
    assert any("Starting data load" in msg for call in mock_logger.info.call_args_list for msg in [call[0][0]])
    assert any("Data load completed" in msg for call in mock_logger.info.call_args_list for msg in [call[0][0]])
    assert any("Portafolios dynamodb processing job completed successfully" in msg for msg in log_calls)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
def test_job_generates_unique_job_id(
    mock_get_logger,
    mock_extract,
    mock_transform,
    mock_load,
    spark,
    mock_vars_resource,
    sample_extracted_data,
):
    """Test that job generates unique job_id for each execution."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_extracted_df = sample_extracted_data
    mock_transformed_df = spark.createDataFrame(
        [("TXN001", "ORG1#CANAL1#VEND1", [{"cod_material": "MAT001"}], "2024-01-01T10:00:00.000Z")],
        schema=["pk", "sk", "productos", "fecha_actualizacion"],
    )

    mock_extract.return_value = mock_extracted_df
    mock_transform.return_value = mock_transformed_df

    result1 = portafolios_dynamodb_job(spark, mock_vars_resource)
    time.sleep(1.1)
    result2 = portafolios_dynamodb_job(spark, mock_vars_resource)

    transform_call_1 = mock_transform.call_args_list[0]
    transform_call_2 = mock_transform.call_args_list[1]

    job_id_1 = transform_call_1.kwargs["job_id"]
    job_id_2 = transform_call_2.kwargs["job_id"]

    assert job_id_1.startswith("portafolios_dynamodb_")
    assert job_id_2.startswith("portafolios_dynamodb_")
    assert job_id_1 != job_id_2
