import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from quind_demo_ppd_project.flows.portafolios_dynamodb.job import portafolios_dynamodb_job
from quind_demo_ppd_project.libs.common_patterns import current_timestamp_with_tz
from quind_demo_ppd_project.libs.runner.types import Status


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test.catalog.portafolios"
    mock_vars.vars.output.table_id = "test.output.portafolios"
    mock_vars.vars.get.return_value = "portafolios_dynamodb"
    return mock_vars


@pytest.fixture
def sample_dataframe(spark):
    """Create sample DataFrame for testing."""
    data = [
        ("1", "Portfolio A", 1000.50),
        ("2", "Portfolio B", 3000.75),
    ]
    return spark.createDataFrame(
        data,
        schema=["id", "nombre", "valor"],
    )


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
def test_job_full_pipeline(
    mock_extract,
    mock_transform,
    mock_load,
    spark,
    mock_vars_resource,
    sample_dataframe,
):
    """Test full ETL pipeline execution."""
    mock_extract.return_value = sample_dataframe
    transformed_df = sample_dataframe.withColumn(
        "current_timestamp_dwh",
        current_timestamp_with_tz("yyyy-MM-dd HH:mm:ss", "America/Bogota"),
    )
    mock_transform.return_value = transformed_df

    status = portafolios_dynamodb_job(spark, mock_vars_resource)

    mock_extract.assert_called_once_with(
        spark=spark,
        vars_instance=mock_vars_resource,
    )

    assert mock_transform.called
    transform_call_args = mock_transform.call_args
    assert transform_call_args.kwargs["job_id"].startswith("portafolios_dynamodb_")
    assert transform_call_args.kwargs["spark"] == spark
    assert transform_call_args.kwargs["vars_instance"] == mock_vars_resource
    assert transform_call_args.kwargs["extracted_data"] == sample_dataframe

    assert mock_load.called
    load_call_args = mock_load.call_args
    assert load_call_args.kwargs["job_id"].startswith("portafolios_dynamodb_")
    assert load_call_args.kwargs["spark"] == spark
    assert load_call_args.kwargs["vars_instance"] == mock_vars_resource
    assert load_call_args.kwargs["transformed_data"] == transformed_df

    assert isinstance(status, Status)
    assert status.status_value == "OK"
    assert "completed successfully" in status.message.lower()


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.load")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.transform")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.job.extract")
def test_job_empty_data(
    mock_extract,
    mock_transform,
    mock_load,
    spark,
    mock_vars_resource,
):
    """Test job execution with empty data."""
    empty_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("nombre", StringType(), True),
            StructField("valor", DoubleType(), True),
        ]
    )
    empty_df = spark.createDataFrame([], schema=empty_schema)

    mock_extract.return_value = empty_df
    mock_transform.return_value = empty_df

    status = portafolios_dynamodb_job(spark, mock_vars_resource)

    mock_extract.assert_called_once()
    mock_transform.assert_called_once()
    mock_load.assert_called_once()

    assert isinstance(status, Status)
    assert status.status_value == "OK"


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
    sample_dataframe,
):
    """Test job generates unique job_id for each execution."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_extract.return_value = sample_dataframe
    mock_transform.return_value = sample_dataframe

    status1 = portafolios_dynamodb_job(spark, mock_vars_resource)
    status2 = portafolios_dynamodb_job(spark, mock_vars_resource)

    transform_call1 = mock_transform.call_args_list[0]
    transform_call2 = mock_transform.call_args_list[1]

    job_id1 = transform_call1.kwargs["job_id"]
    job_id2 = transform_call2.kwargs["job_id"]

    assert job_id1 != job_id2
    assert job_id1.startswith("portafolios_dynamodb_")
    assert job_id2.startswith("portafolios_dynamodb_")

    assert status1.status_value == "OK"
    assert status2.status_value == "OK"
