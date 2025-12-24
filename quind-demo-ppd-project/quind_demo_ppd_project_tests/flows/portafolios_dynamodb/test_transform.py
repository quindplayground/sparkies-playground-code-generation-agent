import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, ArrayType
from pyspark.testing.utils import assertDataFrameEqual

from quind_demo_ppd_project.flows.portafolios_dynamodb.transform import transform


@pytest.fixture
def mock_vars_resource():
    """Create mock VarsResource."""
    mock_vars = MagicMock()
    mock_vars.vars.input.table_id = "test.catalog.database.portafolios"
    mock_vars.vars.output.table_id = "test.output.table"
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
            "TXN001",
            "ORG1",
            "CANAL1",
            "VEND1",
            True,
            True,
            "2024-01-01 11:00:00",
            "MAT002",
            "Product2",
        ),
        (
            "TXN002",
            "ORG2",
            "CANAL2",
            "VEND2",
            True,
            True,
            "2024-01-02 10:00:00",
            "MAT003",
            "Product3",
        ),
        (
            "TXN003",
            "ORG3",
            "CANAL3",
            "VEND3",
            False,
            True,
            "2024-01-03 10:00:00",
            "MAT004",
            "Product4",
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


def test_transform_happy_path(spark, mock_vars_resource, sample_extracted_data):
    """Test transformation pipeline with valid data."""
    job_id = "test_job_123"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.transform.get_logger"
    ) as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        result = transform(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars_resource,
            extracted_data=sample_extracted_data,
        )

        assert isinstance(result, DataFrame)
        assert "pk" in result.columns
        assert "sk" in result.columns
        assert "gsi1_pk" in result.columns
        assert "gsi1_sk" in result.columns
        assert "gsi2_pk" in result.columns
        assert "gsi2_sk" in result.columns
        assert "productos" in result.columns
        assert "fecha_actualizacion" in result.columns

        result_rows = result.collect()
        assert len(result_rows) == 2

        txn001_row = [r for r in result_rows if r["pk"] == "TXN001"][0]
        assert txn001_row["pk"] == "TXN001"
        assert txn001_row["sk"] == "ORG1#CANAL1#VEND1"
        assert txn001_row["gsi1_pk"] == "ORG1"
        assert txn001_row["gsi1_sk"] == "CANAL1#TXN001"
        assert txn001_row["gsi2_pk"] == "VEND1"
        assert txn001_row["gsi2_sk"] == "TXN001"
        assert len(txn001_row["productos"]) == 2


def test_transform_filters_inactive_records(spark, mock_vars_resource, sample_extracted_data):
    """Test that transformation filters out inactive records."""
    job_id = "test_job_123"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.transform.get_logger"
    ) as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        result = transform(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars_resource,
            extracted_data=sample_extracted_data,
        )

        result_rows = result.collect()
        pk_values = [r["pk"] for r in result_rows]
        assert "TXN003" not in pk_values
        assert "TXN001" in pk_values
        assert "TXN002" in pk_values


def test_transform_empty_dataframe(spark, mock_vars_resource):
    """Test transformation with empty DataFrame."""
    empty_schema = StructType([
        StructField("cod_transaccional", StringType(), True),
        StructField("cod_org_vent", StringType(), True),
        StructField("cod_canal", StringType(), True),
        StructField("cod_vendedor", StringType(), True),
        StructField("ind_cliente_activo", BooleanType(), True),
        StructField("ind_material_activo", BooleanType(), True),
        StructField("fec_actualizacion_dl", StringType(), True),
        StructField("cod_material", StringType(), True),
        StructField("des_material", StringType(), True),
    ])
    empty_df = spark.createDataFrame([], schema=empty_schema)

    job_id = "test_job_123"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.transform.get_logger"
    ) as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        result = transform(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars_resource,
            extracted_data=empty_df,
        )

        assert isinstance(result, DataFrame)
        assert result.count() == 0


def test_transform_aggregates_productos(spark, mock_vars_resource, sample_extracted_data):
    """Test that productos are properly aggregated by client combination."""
    job_id = "test_job_123"

    with patch(
        "quind_demo_ppd_project.flows.portafolios_dynamodb.transform.get_logger"
    ) as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        result = transform(
            job_id=job_id,
            spark=spark,
            vars_instance=mock_vars_resource,
            extracted_data=sample_extracted_data,
        )

        result_rows = result.collect()
        txn001_row = [r for r in result_rows if r["pk"] == "TXN001"][0]

        assert isinstance(txn001_row["productos"], list)
        assert len(txn001_row["productos"]) == 2

        productos_codes = [p["cod_material"] for p in txn001_row["productos"]]
        assert "MAT001" in productos_codes
        assert "MAT002" in productos_codes
