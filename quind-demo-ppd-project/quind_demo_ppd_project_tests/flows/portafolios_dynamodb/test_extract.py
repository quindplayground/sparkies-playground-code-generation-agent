import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

from quind_demo_ppd_project.flows.portafolios_dynamodb.extract import extract


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test.catalog.database.portafolios"
    return mock_vars


def test_extract_full_load(spark, mock_vars_resource):
    """Test full extraction from source table."""
    expected_data = [
        ("1", "ORG1", "CANAL1", "VEND1", True, True, "2024-01-01"),
        ("2", "ORG2", "CANAL2", "VEND2", True, True, "2024-01-02"),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=[
            "cod_transaccional",
            "cod_org_vent",
            "cod_canal",
            "cod_vendedor",
            "ind_cliente_activo",
            "ind_material_activo",
            "fec_actualizacion_dl",
        ],
    )

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = expected_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, expected_df)
        assertSchemaEqual(result.schema, expected_df.schema)
        mock_table.assert_called_once_with("test.catalog.database.portafolios")


def test_extract_empty_source(spark, mock_vars_resource):
    """Test extraction with empty source table."""
    empty_schema = StructType([
        StructField("cod_transaccional", StringType(), True),
        StructField("cod_org_vent", StringType(), True),
        StructField("cod_canal", StringType(), True),
        StructField("cod_vendedor", StringType(), True),
        StructField("ind_cliente_activo", BooleanType(), True),
        StructField("ind_material_activo", BooleanType(), True),
        StructField("fec_actualizacion_dl", StringType(), True),
    ])
    empty_df = spark.createDataFrame([], schema=empty_schema)

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = empty_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, empty_df)
        assert result.count() == 0


def test_extract_with_different_schema(spark, mock_vars_resource):
    """Test extraction preserves source schema."""
    source_data = [
        ("1", "ORG1", "CANAL1", "VEND1", True, True, "2024-01-01", 100.5),
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
            "fec_actualizacion_dl",
            "amount",
        ],
    )

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = source_df

        result = extract(spark, mock_vars_resource)

        assertSchemaEqual(result.schema, source_df.schema)
        assertDataFrameEqual(result, source_df)
