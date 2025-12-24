"""Load module for portafolios_dynamodb flow.

This module handles loading transformed portfolio data to DynamoDB using the
DynamoDBLoader utility. The load strategy uses overwrite mode for the first run
and supports incremental updates for subsequent runs.
"""

from pyspark.sql import DataFrame, SparkSession

from quind_demo_ppd_project.libs.aws.dynamodb.loader import DynamoDBLoader
from quind_demo_ppd_project.libs.error_handler import handle_errors
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource

logger = get_logger(__name__)


@handle_errors
def load(
    job_id: str,
    spark: SparkSession,
    vars_instance: VarsResource,
    transformed_data: DataFrame,
    **kwargs
) -> None:
    """Load transformed portfolio data to DynamoDB.

    This function loads the transformed DataFrame to DynamoDB using the DynamoDBLoader.
    The load strategy is determined by the first_run parameter:
    - first_run=True: Only writes data (overwrite behavior)
    - first_run=False: Writes new/updated data and deletes removed records

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.output.table_name: DynamoDB table name
            - vars_instance.vars.output.region: AWS region for DynamoDB table
            - vars_instance.vars.output.item_size_limit: Maximum item size in bytes
        transformed_data: Transformed DataFrame to load with columns:
            - pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk: DynamoDB keys
            - productos: Array of material records
            - fecha_actualizacion: ISO 8601 timestamp
        **kwargs: Optional parameters.
            - first_run (bool): Whether this is the first execution (default: True).
                If True, only writes data. If False, also processes deletions.
    """
    table_name = vars_instance.vars.output.table_name
    region = vars_instance.vars.output.region
    item_size_limit = vars_instance.vars.output.item_size_limit
    first_run = kwargs.get("first_run", True)

    logger.info(
        "Loading data to DynamoDB",
        extra={
            "attributes": {
                "job_id": job_id,
                "table_name": table_name,
                "region": region,
                "item_size_limit": item_size_limit,
                "first_run": first_run,
                "load_strategy": "overwrite" if first_run else "merge",
                "rows_to_write": transformed_data.count(),
            }
        },
    )

    loader = DynamoDBLoader(
        job_id=job_id,
        vars_instance=vars_instance,
    )

    empty_delete_schema = transformed_data.select("pk", "sk").schema
    empty_delete_df = spark.createDataFrame([], schema=empty_delete_schema)

    loader.load(
        dataframes={
            "data_to_write": transformed_data,
            "data_to_delete": empty_delete_df,
        },
        first_run=first_run,
        write_handler="portfolio_writer",
        delete_handler="delete",
    )

    logger.info(
        "Data loaded to DynamoDB successfully",
        extra={
            "attributes": {
                "job_id": job_id,
                "table_name": table_name,
                "region": region,
                "first_run": first_run,
                "load_strategy": "overwrite" if first_run else "merge",
            }
        },
    )
