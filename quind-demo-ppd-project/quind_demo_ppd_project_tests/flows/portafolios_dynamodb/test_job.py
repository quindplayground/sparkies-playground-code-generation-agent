import pytest
from unittest.mock import MagicMock, patch, ANY
from pyspark.sql import SparkSession
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
    }.get(key, default))
    mock_vars.vars.input.table_id = "test_catalog.test_schema.test_table"
    mock_vars.vars.output.table_id = "test_output_table"
    mock_vars.vars.output.table_name = "test-output-table"
    mock_vars.vars.output.region = "us-east-1"
    return mock_vars


@pytest.fixture
def sample_extracted_data(spark):
    """Create sample extracted data DataFrame."""
    data = [
        ("CLI001", "MAT001", "ORG001", "CAN001", "VEN001", True, True, "2024-01-01 10:00:00"),
        ("CLI001", "MAT002", "ORG001", "CAN001", "VEN001", True, True, "2024-01-01 11:00:00"),
    ]
    schema = StructType([
        StructField("cod_transaccional", StringType(), nullable=True),
        StructField("cod_material", StringType(), nullable=True),
        StructField("cod_org_vent", StringType(), nullable=True),
        StructField("cod_canal", StringType(), nullable=True),
        StructField("cod_vendedor", StringType(), nullable=True),
        StructField("ind_cliente_activo", StringType(), nullable=True),
        StructField("ind_material_activo", StringType(), nullable=True),
        StructField("fec_actualizacion_dl", StringType(), nullable=True),
    ])
    return spark.createDataFrame(data, schema=schema)


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
def test_job_success(
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test successful job execution."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "OK"
    assert "completed successfully" in result.message

    mock_extract.assert_called_once_with(
        spark=spark,
        vars_instance=mock_vars_resource,
    )
    mock_transform.assert_called_once_with(
        job_id=ANY,
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=sample_extracted_data,
    )
    mock_load.assert_called_once_with(
        job_id=ANY,
        spark=spark,
        vars_instance=mock_vars_resource,
        transformed_data=sample_transformed_data,
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
def test_job_logging(
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

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    portafolios_dynamodb_job(spark, mock_vars_resource)

    assert mock_logger.info.call_count >= 6
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Starting portafolios dynamodb processing job" in msg for msg in log_calls)
    assert any("Starting data extraction" in msg for msg in log_calls)
    assert any("Data extraction completed" in msg for msg in log_calls)
    assert any("Starting data transformation" in msg for msg in log_calls)
    assert any("Data transformation completed" in msg for msg in log_calls)
    assert any("Starting data load" in msg for msg in log_calls)
    assert any("Data load completed" in msg for msg in log_calls)
    assert any("Portafolios dynamodb processing job completed successfully" in msg for msg in log_calls)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
def test_job_uses_component_name_from_config(
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test that job uses component_name from config."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_vars_resource.vars.get = MagicMock(side_effect=lambda key, default=None: {
        "component_name": "custom_component_name",
    }.get(key, default))

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    portafolios_dynamodb_job(spark, mock_vars_resource)

    found_component = False
    for call in mock_logger.info.call_args_list:
        if len(call[1]) > 0 and "extra" in call[1]:
            attributes = call[1]["extra"].get("attributes", {})
            if attributes.get("component") == "custom_component_name":
                found_component = True
                break
    
    assert found_component, "Component name 'custom_component_name' not found in log calls"


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
def test_job_generates_job_id(
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test that job generates a unique job_id."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    portafolios_dynamodb_job(spark, mock_vars_resource)

    transform_call = mock_transform.call_args
    load_call = mock_load.call_args

    job_id_transform = transform_call[1]["job_id"]
    job_id_load = load_call[1]["job_id"]

    assert job_id_transform == job_id_load
    assert job_id_transform.startswith("portafolios_dynamodb_")
    assert len(job_id_transform) > len("portafolios_dynamodb_")


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
def test_job_returns_status_object(
    mock_extract,
    mock_transform,
    mock_load,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
    sample_transformed_data,
):
    """Test that job returns Status object with correct values."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_extract.return_value = sample_extracted_data
    mock_transform.return_value = sample_transformed_data

    result = portafolios_dynamodb_job(spark, mock_vars_resource)

    assert isinstance(result, Status)
    assert result.status_value == "OK"
    assert isinstance(result.message, str)
    assert len(result.message) > 0
