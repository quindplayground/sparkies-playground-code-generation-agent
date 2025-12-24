"""Transform module for portafolios_dynamodb flow.

This module handles data transformation logic for portafolios data being
processed from Iceberg to DynamoDB. The transformations prepare the data
for loading into DynamoDB, including cleaning, type casting, and metadata
addition.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf

from quind_demo_ppd_project.libs.common_patterns import current_timestamp_with_tz
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
    """Transform portafolios data for DynamoDB loading.

    This function applies transformations to prepare portafolios data from
    Iceberg for loading into DynamoDB. The transformations include:
    - Data cleaning (deduplication)
    - Type casting and validation
    - Metadata addition (timestamps)

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
        extracted_data: Input DataFrame from extraction step.
        **kwargs: Optional parameters based on flow requirements.

    Returns:
        Fully transformed DataFrame ready for DynamoDB loading.

    Example:
        ```python
        transformed_data = transform(
            job_id="portafolios_1234567890",
            spark=spark,
            vars_instance=vars_instance,
            extracted_data=extracted_data
        )
        ```
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

    # TODO(user): Define primary key columns for deduplication based on requirements
    # Example: primary_keys = ["portafolio_id", "fecha"]
    # For now, using all columns for deduplication
    step1 = extracted_data.dropDuplicates()

    logger.info(
        "Deduplication completed",
        extra={
            "attributes": {
                "job_id": job_id,
            }
        },
    )

    # TODO(user): Define date/timestamp columns that need type casting based on requirements
    # Example: step2 = step1.withColumn("fecha", sf.col("fecha").cast("date"))
    step2 = step1

    # Add metadata timestamp for tracking
    step3 = step2.withColumn(
        "current_timestamp_dwh",
        current_timestamp_with_tz("yyyy-MM-dd HH:mm:ss", "America/Bogota"),
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

    return step3
