import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, BinaryType
from datetime import datetime

from quind_demo_ppd_project.flows.portafolios_dynamodb.transform import transform
from quind_demo_ppd_project.libs.resources import VarsResource


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock(spec=VarsResource)
    mock_vars.vars.input.table_id = "test.catalog.database.portafolios"
    mock_vars.vars.output.table_id = "test.catalog.database.portafolios_output"
    return mock_vars


def test_transform_full_pipeline(spark, mock_vars_resource):
    """Test full transformation pipeline."""
    from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_100_filter_active import (
        step_100_filter_active,
    )
    from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_200_build_keys import (
        step_200_build_keys,
    )
    from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_300_aggregate_products import (
        step_300_aggregate_products,
    )
    from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_400_format_timestamp import (
        step_400_format_timestamp,
    )

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
        (
            "TXN1",
            "ORG1",
            "CANAL1",
            "VEND1",
            True,
            True,
            "MAT2",
            datetime(2024, 1, 1, 12, 0, 0),
        ),
        (
            "TXN2",
            "ORG2",
            "CANAL2",
            "VEND2",
            True,
            True,
            "MAT3",
            datetime(2024, 1, 2, 12, 0, 0),
        ),
    ]
    input_df = spark.createDataFrame(
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

    job_id = "test_job_123"

    result = transform(
        job_id=job_id,
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=input_df,
    )

    expected_columns = [
        "pk",
        "sk",
        "gsi1_pk",
        "gsi1_sk",
        "gsi2_pk",
        "gsi2_sk",
        "productos",
        "fecha_actualizacion",
    ]
    assert set(result.columns) == set(expected_columns)
    assert result.count() == 2


def test_transform_filters_inactive_records(spark, mock_vars_resource):
    """Test that transform filters out inactive records."""
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
        (
            "TXN2",
            "ORG2",
            "CANAL2",
            "VEND2",
            False,
            True,
            "MAT2",
            datetime(2024, 1, 2, 12, 0, 0),
        ),
        (
            "TXN3",
            "ORG3",
            "CANAL3",
            "VEND3",
            True,
            False,
            "MAT3",
            datetime(2024, 1, 3, 12, 0, 0),
        ),
    ]
    input_df = spark.createDataFrame(
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

    job_id = "test_job_456"

    result = transform(
        job_id=job_id,
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=input_df,
    )

    assert result.count() == 1
    assert result.select("pk").first()[0] == "TXN1"


def test_transform_empty_dataframe(spark, mock_vars_resource):
    """Test transform with empty DataFrame."""
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
                StructField("cod_material", StringType(), True),
                StructField("fec_actualizacion_dl", TimestampType(), True),
            ]
        ),
    )

    job_id = "test_job_empty"

    result = transform(
        job_id=job_id,
        spark=spark,
        vars_instance=mock_vars_resource,
        extracted_data=empty_df,
    )

    assert result.count() == 0
    expected_columns = [
        "pk",
        "sk",
        "gsi1_pk",
        "gsi1_sk",
        "gsi2_pk",
        "gsi2_sk",
        "productos",
        "fecha_actualizacion",
    ]
    assert set(result.columns) == set(expected_columns)
