"""Load module for portafolios_dynamodb flow.

This module handles loading transformed data to DynamoDB using the DynamoDBLoader.
The load strategy uses UPSERT operations (write) and supports DELETE operations
for incremental loads when first_run=False.
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
    """Load transformed data to DynamoDB.

    This function loads transformed portafolios data to DynamoDB using the
    DynamoDBLoader. All transformed records are written to DynamoDB using
    UPSERT operations. DELETE operations are only executed when first_run=False.

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.output.table_name: DynamoDB table name
            - vars_instance.vars.output.region: AWS region for DynamoDB table
            - vars_instance.vars.output.item_size_limit: Maximum item size in bytes
        transformed_data: Transformed DataFrame with columns:
            - pk: Primary key (cod_transaccional)
            - sk: Sort key (org#canal#vendedor)
            - gsi1_pk: GSI1 partition key
            - gsi1_sk: GSI1 sort key
            - gsi2_pk: GSI2 partition key
            - gsi2_sk: GSI2 sort key
            - productos: JSON string array of materials
            - fecha_actualizacion: ISO formatted timestamp
        **kwargs: Optional parameters.
            - first_run (bool): If True, only write operations are executed.
              If False, both write and delete operations are executed.
              Defaults to True.
    """
    first_run = kwargs.get("first_run", True)

    table_name = vars_instance.vars.output.table_name
    region = getattr(vars_instance.vars.output, "region", None)
    item_size_limit = getattr(
        vars_instance.vars.output, "item_size_limit", 389120
    )

    if region is None:
        raise ValueError(
            "DynamoDB region is required. "
            "Please configure 'region' in [default.output] section of config/default.toml"
        )

    logger.info(
        "Loading data to DynamoDB",
        extra={
            "attributes": {
                "job_id": job_id,
                "table_name": table_name,
                "region": region,
                "item_size_limit": item_size_limit,
                "first_run": first_run,
                "load_strategy": "upsert",
            }
        },
    )

    loader = DynamoDBLoader(
        job_id=job_id,
        vars_instance=vars_instance,
    )

    data_to_write = transformed_data
    data_to_delete = spark.createDataFrame([], schema=transformed_data.schema)

    loader.load(
        dataframes={
            "data_to_write": data_to_write,
            "data_to_delete": data_to_delete,
        },
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
                "first_run": first_run,
                "load_strategy": "upsert",
            }
        },
    )
