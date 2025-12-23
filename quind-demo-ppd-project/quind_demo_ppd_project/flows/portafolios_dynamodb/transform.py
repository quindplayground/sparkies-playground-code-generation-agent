"""Transform module for portafolios_dynamodb flow.

This module handles data transformation logic for portfolio data.
Transforms Iceberg table data into DynamoDB-ready format with:
- Active record filtering
- Grouping by client-portfolio combination
- Productos array aggregation and compression
- DynamoDB key construction
"""

from pyspark.sql import DataFrame, SparkSession

from quind_demo_ppd_project.libs.error_handler import handle_errors
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps import (
    step_100_filter_active_records,
    step_200_group_and_aggregate,
    step_300_build_dynamodb_keys,
    step_400_compress_productos,
)

logger = get_logger(__name__)


@handle_errors
def transform(
    job_id: str,
    spark: SparkSession,
    vars_instance: VarsResource,
    extracted_data: DataFrame,
    **kwargs
) -> DataFrame:
    """Transform data through transformation pipeline.

    Orchestrates transformation steps to convert Iceberg portfolio data
    into DynamoDB-ready format:
    1. Filter active records (ind_cliente_activo AND ind_material_activo = true)
    2. Group by cod_transaccional + cod_org_vent + cod_canal + cod_vendedor
    3. Aggregate productos array (excluding specified columns)
    4. Build DynamoDB keys (pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk)
    5. Compress productos array to GZIP JSON
    6. Extract fecha_actualizacion as ISO 8601 timestamp

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
        extracted_data: Input DataFrame from extraction step.
        **kwargs: Optional parameters (not used in this flow).

    Returns:
        Fully transformed DataFrame ready for DynamoDB loading with columns:
        - pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk (DynamoDB keys)
        - productos (GZIP compressed JSON bytes)
        - fecha_actualizacion (ISO 8601 timestamp string)
    """
    input_table_id = vars_instance.vars.input.table_id
    output_table_id = vars_instance.vars.output.table_id

    logger.info(
        "Transformation started",
        extra={
            "attributes": {
                "job_id": job_id,
                "input_table_id": input_table_id,
                "output_table_id": output_table_id,
            }
        },
    )

    step_100 = step_100_filter_active_records(extracted_data)
    logger.info(
        "Step 100 completed: Filtered active records",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_100_filter_active_records",
            }
        },
    )

    step_200 = step_200_group_and_aggregate(step_100)
    logger.info(
        "Step 200 completed: Grouped and aggregated productos",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_200_group_and_aggregate",
            }
        },
    )

    step_300 = step_300_build_dynamodb_keys(step_200)
    logger.info(
        "Step 300 completed: Built DynamoDB keys",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_300_build_dynamodb_keys",
            }
        },
    )

    step_400 = step_400_compress_productos(step_300)
    logger.info(
        "Step 400 completed: Compressed productos to GZIP JSON",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_400_compress_productos",
            }
        },
    )

    final_columns = [
        "pk",
        "sk",
        "gsi1_pk",
        "gsi1_sk",
        "gsi2_pk",
        "gsi2_sk",
        "productos",
        "fecha_actualizacion",
    ]

    result = step_400.select(*final_columns)

    logger.info(
        "Transformation completed",
        extra={
            "attributes": {
                "job_id": job_id,
                "input_table_id": input_table_id,
                "output_table_id": output_table_id,
            }
        },
    )

    return result
