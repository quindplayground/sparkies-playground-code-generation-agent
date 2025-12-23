import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

from quind_demo_ppd_project.flows.portafolios_dynamodb.transform import transform


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource for testing."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test_catalog.test_schema.test_table"
    mock_vars.vars.output.table_id = "test_output_table"
    return mock_vars


@pytest.fixture
def sample_extracted_data(spark):
    """Create sample extracted data DataFrame."""
    data = [
        ("CLI001", "MAT001", "ORG001", "CAN001", "VEN001", True, True, "2024-01-01 10:00:00"),
        ("CLI001", "MAT002", "ORG001", "CAN001", "VEN001", True, True, "2024-01-01 11:00:00"),
        ("CLI002", "MAT001", "ORG001", "CAN001", "VEN001", True, False, "2024-01-01 12:00:00"),
        ("CLI001", "MAT003", "ORG001", "CAN001", "VEN001", False, True, "2024-01-01 13:00:00"),
    ]
    schema = StructType([
        StructField("cod_transaccional", StringType(), nullable=True),
        StructField("cod_material", StringType(), nullable=True),
        StructField("cod_org_vent", StringType(), nullable=True),
        StructField("cod_canal", StringType(), nullable=True),
        StructField("cod_vendedor", StringType(), nullable=True),
        StructField("ind_cliente_activo", BooleanType(), nullable=True),
        StructField("ind_material_activo", BooleanType(), nullable=True),
        StructField("fec_actualizacion_dl", StringType(), nullable=True),
    ])
    return spark.createDataFrame(data, schema=schema)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_400_format_output")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_300_build_keys")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_200_group_and_aggregate")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_100_filter_active")
def test_transform_pipeline_execution(
    mock_step_100,
    mock_step_200,
    mock_step_300,
    mock_step_400,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
):
    """Test that transform executes all steps in correct order."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    filtered_df = sample_extracted_data.filter("ind_cliente_activo = true AND ind_material_activo = true")
    mock_step_100.return_value = filtered_df

    aggregated_df = spark.createDataFrame(
        [("CLI001", "ORG001", "CAN001", "VEN001", [], "2024-01-01 11:00:00")],
        schema=StructType([
            StructField("cod_transaccional", StringType(), nullable=True),
            StructField("cod_org_vent", StringType(), nullable=True),
            StructField("cod_canal", StringType(), nullable=True),
            StructField("cod_vendedor", StringType(), nullable=True),
            StructField("productos", ArrayType(StructType([])), nullable=True),
            StructField("fec_actualizacion_dl", StringType(), nullable=True),
        ])
    )
    mock_step_200.return_value = aggregated_df

    keys_df = spark.createDataFrame(
        [("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", [], "2024-01-01 11:00:00")],
        schema=StructType([
            StructField("cod_transaccional", StringType(), nullable=True),
            StructField("sk", StringType(), nullable=True),
            StructField("gsi1_pk", StringType(), nullable=True),
            StructField("gsi1_sk", StringType(), nullable=True),
            StructField("gsi2_pk", StringType(), nullable=True),
            StructField("gsi2_sk", StringType(), nullable=True),
            StructField("productos", ArrayType(StructType([])), nullable=True),
            StructField("fec_actualizacion_dl", StringType(), nullable=True),
        ])
    )
    mock_step_300.return_value = keys_df

    final_df = spark.createDataFrame(
        [("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", "[]", "2024-01-01T11:00:00.000Z")],
        schema=["pk", "sk", "gsi1_pk", "gsi1_sk", "gsi2_pk", "gsi2_sk", "productos", "fecha_actualizacion"]
    )
    mock_step_400.return_value = final_df

    result = transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=sample_extracted_data
    )

    assertDataFrameEqual(result, final_df)
    mock_step_100.assert_called_once_with(sample_extracted_data)
    mock_step_200.assert_called_once_with(filtered_df)
    mock_step_300.assert_called_once_with(aggregated_df)
    mock_step_400.assert_called_once_with(keys_df)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_400_format_output")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_300_build_keys")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_200_group_and_aggregate")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_100_filter_active")
def test_transform_empty_input(
    mock_step_100,
    mock_step_200,
    mock_step_300,
    mock_step_400,
    mock_get_logger,
    spark,
    mock_vars_resource,
):
    """Test transform with empty input DataFrame."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    empty_df = spark.createDataFrame([], schema=StructType([
        StructField("cod_transaccional", StringType(), nullable=True),
        StructField("cod_material", StringType(), nullable=True),
        StructField("cod_org_vent", StringType(), nullable=True),
        StructField("cod_canal", StringType(), nullable=True),
        StructField("cod_vendedor", StringType(), nullable=True),
        StructField("ind_cliente_activo", BooleanType(), nullable=True),
        StructField("ind_material_activo", BooleanType(), nullable=True),
        StructField("fec_actualizacion_dl", StringType(), nullable=True),
    ]))

    mock_step_100.return_value = empty_df
    mock_step_200.return_value = empty_df
    mock_step_300.return_value = empty_df
    mock_step_400.return_value = empty_df

    result = transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=empty_df
    )

    assertDataFrameEqual(result, empty_df)
    assert result.count() == 0


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_400_format_output")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_300_build_keys")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_200_group_and_aggregate")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_100_filter_active")
def test_transform_logging(
    mock_step_100,
    mock_step_200,
    mock_step_300,
    mock_step_400,
    mock_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
):
    """Test that transform logs correctly at each step."""

    filtered_df = sample_extracted_data.filter("ind_cliente_activo = true AND ind_material_activo = true")
    mock_step_100.return_value = filtered_df

    aggregated_df = spark.createDataFrame(
        [("CLI001", "ORG001", "CAN001", "VEN001", [], "2024-01-01 11:00:00")],
        schema=StructType([
            StructField("cod_transaccional", StringType(), nullable=True),
            StructField("cod_org_vent", StringType(), nullable=True),
            StructField("cod_canal", StringType(), nullable=True),
            StructField("cod_vendedor", StringType(), nullable=True),
            StructField("productos", ArrayType(StructType([])), nullable=True),
            StructField("fec_actualizacion_dl", StringType(), nullable=True),
        ])
    )
    mock_step_200.return_value = aggregated_df

    keys_df = spark.createDataFrame(
        [("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", [], "2024-01-01 11:00:00")],
        schema=StructType([
            StructField("cod_transaccional", StringType(), nullable=True),
            StructField("sk", StringType(), nullable=True),
            StructField("gsi1_pk", StringType(), nullable=True),
            StructField("gsi1_sk", StringType(), nullable=True),
            StructField("gsi2_pk", StringType(), nullable=True),
            StructField("gsi2_sk", StringType(), nullable=True),
            StructField("productos", ArrayType(StructType([])), nullable=True),
            StructField("fec_actualizacion_dl", StringType(), nullable=True),
        ])
    )
    mock_step_300.return_value = keys_df

    final_df = spark.createDataFrame(
        [("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", "[]", "2024-01-01T11:00:00.000Z")],
        schema=["pk", "sk", "gsi1_pk", "gsi1_sk", "gsi2_pk", "gsi2_sk", "productos", "fecha_actualizacion"]
    )
    mock_step_400.return_value = final_df

    transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=sample_extracted_data
    )

    assert mock_logger.info.call_count >= 5
    log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Starting data transformation" in msg for msg in log_calls)
    assert any("Step 100" in msg for msg in log_calls)
    assert any("Step 200" in msg for msg in log_calls)
    assert any("Step 300" in msg for msg in log_calls)
    assert any("Step 400" in msg for msg in log_calls)
    assert any("Data transformation completed" in msg for msg in log_calls)


@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.get_logger")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_400_format_output")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_300_build_keys")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_200_group_and_aggregate")
@patch("quind_demo_ppd_project.flows.portafolios_dynamodb.transform.step_100_filter_active")
def test_transform_with_kwargs(
    mock_step_100,
    mock_step_200,
    mock_step_300,
    mock_step_400,
    mock_get_logger,
    spark,
    mock_vars_resource,
    sample_extracted_data,
):
    """Test transform accepts and ignores extra kwargs."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    filtered_df = sample_extracted_data.filter("ind_cliente_activo = true AND ind_material_activo = true")
    mock_step_100.return_value = filtered_df

    aggregated_df = spark.createDataFrame(
        [("CLI001", "ORG001", "CAN001", "VEN001", [], "2024-01-01 11:00:00")],
        schema=StructType([
            StructField("cod_transaccional", StringType(), nullable=True),
            StructField("cod_org_vent", StringType(), nullable=True),
            StructField("cod_canal", StringType(), nullable=True),
            StructField("cod_vendedor", StringType(), nullable=True),
            StructField("productos", ArrayType(StructType([])), nullable=True),
            StructField("fec_actualizacion_dl", StringType(), nullable=True),
        ])
    )
    mock_step_200.return_value = aggregated_df

    keys_df = spark.createDataFrame(
        [("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", [], "2024-01-01 11:00:00")],
        schema=StructType([
            StructField("cod_transaccional", StringType(), nullable=True),
            StructField("sk", StringType(), nullable=True),
            StructField("gsi1_pk", StringType(), nullable=True),
            StructField("gsi1_sk", StringType(), nullable=True),
            StructField("gsi2_pk", StringType(), nullable=True),
            StructField("gsi2_sk", StringType(), nullable=True),
            StructField("productos", ArrayType(StructType([])), nullable=True),
            StructField("fec_actualizacion_dl", StringType(), nullable=True),
        ])
    )
    mock_step_300.return_value = keys_df

    final_df = spark.createDataFrame(
        [("CLI001", "ORG001#CAN001#VEN001", "ORG001", "CAN001#CLI001", "VEN001", "CLI001", "[]", "2024-01-01T11:00:00.000Z")],
        schema=["pk", "sk", "gsi1_pk", "gsi1_sk", "gsi2_pk", "gsi2_sk", "productos", "fecha_actualizacion"]
    )
    mock_step_400.return_value = final_df

    result = transform(
        job_id="test_job_123",
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=sample_extracted_data,
        extra_param="ignored"
    )

    assertDataFrameEqual(result, final_df)
