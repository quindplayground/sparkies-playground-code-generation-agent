"""Transform module for portafolios_dynamodb flow.

This module handles data transformation logic for processing portafolios
from Iceberg to DynamoDB. The transformation pipeline includes:
- Filtering active records
- Grouping and aggregating products
- Building DynamoDB keys
- Formatting output for DynamoDB
"""

from pyspark.sql import DataFrame, SparkSession

from quind_demo_ppd_project.libs.error_handler import handle_errors
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_100_filter_active import (
    step_100_filter_active,
)
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_200_group_and_aggregate import (
    step_200_group_and_aggregate,
)
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_300_build_keys import (
    step_300_build_keys,
)
from quind_demo_ppd_project.flows.portafolios_dynamodb.steps.step_400_format_output import (
    step_400_format_output,
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

    This function orchestrates all transformation steps for the portafolios
    flow. It processes extracted data from Iceberg CDC and transforms it
    into the format required for DynamoDB loading.

    Transformation steps:
    1. Filter active records (client and material must be active)
    2. Group by client/org/channel/seller and aggregate products
    3. Build DynamoDB keys (pk, sk, gsi1, gsi2)
    4. Format output for DynamoDB (rename columns, convert to JSON, format dates)

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
        extracted_data: Input DataFrame from extraction step (CDC changelog).
        **kwargs: Optional parameters (not used in this flow).

    Returns:
        Fully transformed DataFrame ready for DynamoDB loading with columns:
            - pk: Primary key (cod_transaccional)
            - sk: Sort key (org#canal#vendedor)
            - gsi1_pk: GSI1 partition key (cod_org_vent)
            - gsi1_sk: GSI1 sort key (canal#cliente)
            - gsi2_pk: GSI2 partition key (cod_vendedor)
            - gsi2_sk: GSI2 sort key (cod_transaccional)
            - productos: JSON string array of materials
            - fecha_actualizacion: ISO formatted timestamp
    """
    logger.info(
        "Starting data transformation",
        extra={
            "attributes": {
                "job_id": job_id,
                "input_table_id": vars_instance.vars.input.table_id,
                "output_table_id": vars_instance.vars.output.table_id,
            }
        },
    )

    logger.info(
        "Step 100: Filtering active records",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_100_filter_active",
            }
        },
    )
    step_100_result = step_100_filter_active(extracted_data)

    logger.info(
        "Step 200: Grouping and aggregating products",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_200_group_and_aggregate",
            }
        },
    )
    step_200_result = step_200_group_and_aggregate(step_100_result)

    logger.info(
        "Step 300: Building DynamoDB keys",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_300_build_keys",
            }
        },
    )
    step_300_result = step_300_build_keys(step_200_result)

    logger.info(
        "Step 400: Formatting output for DynamoDB",
        extra={
            "attributes": {
                "job_id": job_id,
                "step": "step_400_format_output",
            }
        },
    )
    step_400_result = step_400_format_output(step_300_result)

    logger.info(
        "Data transformation completed",
        extra={
            "attributes": {
                "job_id": job_id,
                "input_table_id": vars_instance.vars.input.table_id,
                "output_table_id": vars_instance.vars.output.table_id,
            }
        },
    )

    return step_400_result
