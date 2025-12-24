import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

from quind_demo_ppd_project.flows.portafolios_dynamodb.extract import extract
from quind_demo_ppd_project.libs.resources import VarsResource


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock(spec=VarsResource)
    mock_vars.vars.input.table_id = "test.catalog.database.portafolios"
    mock_vars.vars.input.error_path = "s3://test-bucket/error_logs/"
    return mock_vars


def test_extract_full_load(spark, mock_vars_resource):
    """Test full extraction from source table."""
    source_data = [
        ("1", "ORG1", "CANAL1", "VEND1", True, True, 1000),
        ("2", "ORG2", "CANAL2", "VEND2", True, True, 2000),
    ]
    source_df = spark.createDataFrame(
        source_data,
        schema=[
            "cod_transaccional",
            "cod_org_vent",
            "cod_canal",
            "cod_vendedor",
            "ind_cliente_activo",
            "ind_material_activo",
            "amount",
        ],
    )

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = source_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, source_df)
        mock_table.assert_called_once_with("test.catalog.database.portafolios")


def test_extract_empty_table(spark, mock_vars_resource):
    """Test extraction from empty source table."""
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

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = empty_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, empty_df)
        assert result.count() == 0


def test_extract_with_multiple_columns(spark, mock_vars_resource):
    """Test extraction with multiple columns from source table."""
    source_data = [
        (
            "TXN1",
            "ORG1",
            "CANAL1",
            "VEND1",
            True,
            True,
            "MAT1",
            "2024-01-01",
            1000,
        ),
        (
            "TXN2",
            "ORG2",
            "CANAL2",
            "VEND2",
            True,
            True,
            "MAT2",
            "2024-01-02",
            2000,
        ),
    ]
    source_df = spark.createDataFrame(
        source_data,
        schema=[
            "cod_transaccional",
            "cod_org_vent",
            "cod_canal",
            "cod_vendedor",
            "ind_cliente_activo",
            "ind_material_activo",
            "cod_material",
            "fec_actualizacion_dl",
            "amount",
        ],
    )

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = source_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, source_df)
        assert result.count() == 2
