import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from quind_demo_ppd_project.flows.portafolios_dynamodb.extract import extract


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test.catalog.portafolios"
    return mock_vars


def test_extract_full_load(spark, mock_vars_resource):
    """Test full extraction from source table."""
    expected_data = [
        ("1", "Portfolio A", 1000.50),
        ("2", "Portfolio B", 3000.75),
        ("3", "Portfolio C", 500.25),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "nombre", "valor"],
    )

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = expected_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, expected_df)
        assertSchemaEqual(result.schema, expected_df.schema)
        mock_table.assert_called_once_with("test.catalog.portafolios")


def test_extract_empty_table(spark, mock_vars_resource):
    """Test extraction from empty source table."""
    empty_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("nombre", StringType(), True),
            StructField("valor", DoubleType(), True),
        ]
    )
    expected_df = spark.createDataFrame([], schema=empty_schema)

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = expected_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, expected_df)
        assertSchemaEqual(result.schema, expected_df.schema)
        assert result.count() == 0


def test_extract_with_different_schema(spark, mock_vars_resource):
    """Test extraction with different column types."""
    expected_data = [
        (1, "Portfolio A", 1000.50, True),
        (2, "Portfolio B", 3000.75, False),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        schema=["id", "nombre", "valor", "activo"],
    )

    with patch.object(spark, "table") as mock_table:
        mock_table.return_value = expected_df

        result = extract(spark, mock_vars_resource)

        assertDataFrameEqual(result, expected_df)
        assertSchemaEqual(result.schema, expected_df.schema)
