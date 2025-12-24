"""Load module for portafolios_dynamodb flow.

This module handles loading transformed data to DynamoDB table.
The load strategy uses merge (upsert) based on PK+SK keys.
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
    **kwargs
) -> None:
    """Load transformed data to DynamoDB table using merge strategy.

    This function loads portafolios data to DynamoDB using the DynamoDBLoader.
    The load strategy is merge (upsert) based on PK+SK keys. Records with the
    same PK+SK combination will update existing items in DynamoDB.

    The function prepares data for write and delete operations:
    - data_to_write: Contains all transformed active records
    - data_to_delete: Contains records to delete (inactive records, handled by CDC)

    Args:
        job_id: Unique identifier for this job execution (for logging).
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.output.table_name: DynamoDB table name
            - vars_instance.vars.output.region: AWS region where table is located
            - vars_instance.vars.output.item_size_limit: Maximum item size in bytes
        transformed_data: Transformed DataFrame ready for loading with columns:
            - pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk (DynamoDB keys)
            - productos (GZIP compressed binary JSON array)
            - fecha_actualizacion (ISO 8601 timestamp string)
        **kwargs: Optional parameters.
            - first_run: Whether this is the first execution (default: False).
                If True, only writes data. If False, also processes deletions.

    Raises:
        KeyError: If required configuration (table_name, region, item_size_limit) is missing.
        ValueError: If transformed_data schema doesn't match expected DynamoDB schema.
    """
    # Get configuration
    # The DynamoDBLoader expects these keys in vars_instance.vars.output:
    # - table_name: DynamoDB table name
    # - region: AWS region
    # - item_size_limit: Item size limit in bytes (optional, defaults to 400000)
    # TODO(user): Ensure these configuration keys are defined in config/default.toml
    # Expected format in [default.output] section:
    #   table_name = "@format portafolio-cliente-{env[RESOURCE_SUFFIX]}"
    #   region = "@format {env[AWS_REGION]}" or region = "us-east-1"
    #   item_size_limit = 400000
    try:
        table_name = vars_instance.vars.output.table_name
    except (AttributeError, KeyError):
        raise KeyError(
            "DynamoDB table_name not configured. "
            "Please define table_name in [default.output] section of config/default.toml. "
            "Expected format: portafolio-cliente-{environment}"
        )

    try:
        region = vars_instance.vars.output.region
    except (AttributeError, KeyError):
        raise KeyError(
            "AWS region not configured. "
            "Please define region in [default.output] section of config/default.toml. "
            "Example: region = \"us-east-1\" or region = \"@format {env[AWS_REGION]}\""
        )

    item_size_limit = getattr(
        vars_instance.vars.output, "item_size_limit", 400000
    )

    # Determine if this is the first run
    first_run = kwargs.get("first_run", False)

    logger.info(
        "Loading data to DynamoDB",
        extra={
            "attributes": {
                "job_id": job_id,
                "target_table": table_name,
                "region": region,
                "load_strategy": "merge",
                "first_run": first_run,
                "item_size_limit": item_size_limit,
            }
        },
    )

    # Prepare data for write (all transformed records)
    data_to_write = transformed_data

    # Prepare data for delete (empty DataFrame for now, as extract doesn't handle CDC yet)
    # TODO(user): Implement CDC logic in extract.py to identify records to delete
    # When CDC is implemented, data_to_delete should contain records that:
    # - Were previously active but are now inactive (ind_cliente_activo = false OR ind_material_activo = false)
    # - Have the same pk, sk structure for deletion
    data_to_delete = spark.createDataFrame([], transformed_data.schema)

    # Prepare dataframes dictionary for DynamoDBLoader
    dataframes: InputDynamoDBLoaderDataFrame = {
        "data_to_write": data_to_write,
        "data_to_delete": data_to_delete,
    }

    # Initialize DynamoDBLoader
    loader = DynamoDBLoader(
        job_id=job_id,
        vars_instance=vars_instance,
    )

    # Load data to DynamoDB
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
                "target_table": table_name,
                "region": region,
                "load_strategy": "merge",
                "first_run": first_run,
            }
        },
    )
