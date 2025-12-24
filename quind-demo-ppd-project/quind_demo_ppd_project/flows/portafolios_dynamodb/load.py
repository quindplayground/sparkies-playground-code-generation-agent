"""Load module for portafolios_dynamodb flow.

This module handles loading transformed portafolios data to DynamoDB using
the DynamoDBLoader utility. The load strategy supports both first run
(overwrite) and incremental updates based on the first_run parameter.
"""

from pyspark.sql import DataFrame, SparkSession

from quind_demo_ppd_project.libs.aws.dynamodb.loader import DynamoDBLoader
from quind_demo_ppd_project.libs.aws.dynamodb.loader.types import (
    InputDynamoDBLoaderDataFrame,
)
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
    **kwargs,
) -> None:
    """Load transformed portafolios data to DynamoDB.

    This function loads portafolios data to DynamoDB using the DynamoDBLoader.
    The load strategy supports:
    - First run (first_run=True): Only writes data, no deletions
    - Incremental (first_run=False): Writes new/updated data and deletes removed items

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.output.table_name: DynamoDB table name
            - vars_instance.vars.output.region: AWS region for DynamoDB table
            - vars_instance.vars.output.item_size_limit: Maximum item size in bytes
        transformed_data: Transformed DataFrame ready for DynamoDB loading.
        **kwargs: Optional parameters.
            - first_run (bool): Whether this is the first execution (default: True).
              When True, only writes data. When False, also processes deletions.

    Example:
        ```python
        load(
            job_id="portafolios_1234567890",
            spark=spark,
            vars_instance=vars_instance,
            transformed_data=transformed_data
        )
        ```

        With first_run parameter:
        ```python
        load(
            job_id="portafolios_1234567890",
            spark=spark,
            vars_instance=vars_instance,
            transformed_data=transformed_data,
            first_run=False
        )
        ```
    """
    table_name = vars_instance.vars.output.table_name
    region = vars_instance.vars.output.region
    item_size_limit = vars_instance.vars.output.get("item_size_limit", 400000)
    first_run = kwargs.get("first_run", True)

    logger.info(
        "Loading data to DynamoDB",
        extra={
            "attributes": {
                "job_id": job_id,
                "table_name": table_name,
                "region": region,
                "load_strategy": "first_run" if first_run else "incremental",
                "item_size_limit": item_size_limit,
            }
        },
    )

    loader = DynamoDBLoader(
        job_id=job_id,
        vars_instance=vars_instance,
    )

    empty_delete_df = spark.createDataFrame([], transformed_data.schema)

    dataframes: InputDynamoDBLoaderDataFrame = {
        "data_to_write": transformed_data,
        "data_to_delete": empty_delete_df,
    }

    loader.load(
        dataframes=dataframes,
        first_run=first_run,
        write_handler="portfolio_writer",
        delete_handler="delete",
    )

    logger.info(
        "Data loaded to DynamoDB",
        extra={
            "attributes": {
                "job_id": job_id,
                "table_name": table_name,
                "region": region,
                "load_strategy": "first_run" if first_run else "incremental",
            }
        },
    )
