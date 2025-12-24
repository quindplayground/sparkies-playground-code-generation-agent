"""Transform module for portafolios_dynamodb flow.

This module handles data transformation logic for processing portfolio data
from Iceberg to DynamoDB format. The transformation pipeline includes:
- Filtering active records
- Building DynamoDB keys
- Aggregating productos array
- Formatting dates
"""

from pyspark.sql import DataFrame, SparkSession

from quind_demo_ppd_project.libs.error_handler import handle_errors
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource

from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_100_filter_active import (
    step_100_filter_active,
)
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_200_build_keys import (
    step_200_build_keys,
)
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_300_aggregate_productos import (
    step_300_aggregate_productos,
)
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_400_format_dates import (
    step_400_format_dates,
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
    """Transform portfolio data through transformation pipeline.

    This function orchestrates all transformation steps:
    1. Filter active records (ind_cliente_activo = true AND ind_material_activo = true)
    2. Build DynamoDB keys (pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk)
    3. Aggregate productos array by client combination
    4. Format fecha_actualizacion as ISO 8601 timestamp

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
        extracted_data: Input DataFrame from extraction step.
        **kwargs: Optional parameters based on flow requirements.

    Returns:
        Fully transformed DataFrame ready for loading to DynamoDB with columns:
        - pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk: DynamoDB keys
        - productos: Array of material records (to be compressed to GZIP JSON)
        - fecha_actualizacion: ISO 8601 timestamp
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
                "input_rows": extracted_data.count(),
            }
        },
    )

    step_100 = step_100_filter_active(extracted_data)
    logger.info(
        "Step 100 completed: Filtered active records",
        extra={"attributes": {"job_id": job_id, "rows_after_filter": step_100.count()}},
    )

    step_200 = step_200_build_keys(step_100)
    logger.info(
        "Step 200 completed: Built DynamoDB keys",
        extra={"attributes": {"job_id": job_id}},
    )

    step_300 = step_300_aggregate_productos(step_200)
    logger.info(
        "Step 300 completed: Aggregated productos array",
        extra={
            "attributes": {
                "job_id": job_id,
                "output_rows": step_300.count(),
            }
        },
    )

    step_400 = step_400_format_dates(step_300)
    logger.info(
        "Step 400 completed: Formatted dates",
        extra={"attributes": {"job_id": job_id}},
    )

    logger.info(
        "Transformation completed",
        extra={
            "attributes": {
                "job_id": job_id,
                "input_table_id": input_table_id,
                "output_table_id": output_table_id,
                "output_rows": step_400.count(),
            }
        },
    )

    return step_400
