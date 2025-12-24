"""Extract module for portafolios_dynamodb flow.

This module handles data extraction from the source Iceberg table containing
portafolios data. The extraction reads all data from the source table for
processing and loading into DynamoDB.
"""

from pyspark.sql import DataFrame, SparkSession

from quind_demo_ppd_project.libs.error_handler import handle_errors
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource

logger = get_logger(__name__)


@handle_errors
def extract(
    spark: SparkSession,
    vars_instance: VarsResource,
    **kwargs
) -> DataFrame:
    """Extract data from source Iceberg table.

    This function extracts portafolios data from the source Iceberg table
    specified in the configuration. The extraction reads all available data
    from the source table.

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.input.table_id: Source table identifier
                (format: catalog.database.table_name)
            - vars_instance.vars.input.error_path: Path for error logs
        **kwargs: Optional parameters. Currently not used, but kept for
            future extensibility if incremental extraction is needed.

    Returns:
        DataFrame containing extracted portafolios data from source table.

    Raises:
        AnalysisException: If the source table does not exist or cannot be accessed.
        SparkException: If there is an error reading from the source table.
    """
    source_table_id = vars_instance.vars.input.table_id

    logger.info(
        "Extracting data from source table",
        extra={
            "attributes": {
                "source": source_table_id,
            }
        },
    )

    source_data = spark.table(source_table_id)

    logger.info(
        "Data extraction completed",
        extra={
            "attributes": {
                "source": source_table_id,
            }
        },
    )

    return source_data
