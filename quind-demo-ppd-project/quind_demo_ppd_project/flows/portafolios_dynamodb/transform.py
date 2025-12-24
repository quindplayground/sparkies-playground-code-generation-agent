"""Transform module for portafolios_dynamodb flow.

This module handles data transformation logic for the portafolios DynamoDB flow.
The transformation pipeline includes:
- Filtering active records
- Building DynamoDB keys
- Aggregating products by client combination
- Compressing products to GZIP JSON
- Formatting timestamps
"""

from pyspark.sql import DataFrame, SparkSession

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
from quind_demo_ppd_project.libs.error_handler import handle_errors
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource

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

    This function orchestrates all transformation steps for the portafolios
    DynamoDB flow:
    1. Filter active records (ind_cliente_activo = true AND ind_material_activo = true)
    2. Build DynamoDB keys (pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk)
    3. Aggregate products by combination and compress to GZIP JSON
    4. Format timestamp as ISO 8601

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
        extracted_data: Input DataFrame from extraction step.
        **kwargs: Optional parameters. Currently not used.

    Returns:
        Fully transformed DataFrame ready for loading to DynamoDB with columns:
        - pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk (DynamoDB keys)
        - productos (GZIP compressed binary JSON array)
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

    step_100 = step_100_filter_active(extracted_data)
    logger.info(
        "Step 100: Filter active records completed",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_100_filter_active",
            }
        },
    )

    step_200 = step_200_build_keys(step_100)
    logger.info(
        "Step 200: Build DynamoDB keys completed",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_200_build_keys",
            }
        },
    )

    step_300 = step_300_aggregate_products(step_200)
    logger.info(
        "Step 300: Aggregate products and compress completed",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_300_aggregate_products",
            }
        },
    )

    step_400 = step_400_format_timestamp(step_300)
    logger.info(
        "Step 400: Format timestamp completed",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_400_format_timestamp",
            }
        },
    )

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

    return step_400
