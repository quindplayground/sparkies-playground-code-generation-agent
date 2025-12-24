"""Extract module for portafolios_dynamodb flow.

This module handles data extraction from the source Iceberg table containing
portafolios data. The extraction reads all data from the source table.
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

    Extracts all data from the source Iceberg table specified in the configuration.
    The source table is identified by vars_instance.vars.input.table_id in the
    format catalog.database.table_name.

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.input.table_id: Source table identifier
                (format: catalog.database.table_name)
        **kwargs: Optional parameters for future extensions. Currently unused.

    Returns:
        DataFrame containing extracted data from source table.

    Raises:
        AnalysisException: If the source table does not exist or cannot be accessed.
    """
    source_id = vars_instance.vars.input.table_id

    logger.info(
        "Extracting data from source table",
        extra={
            "attributes": {
                "source": source_id,
            }
        },
    )

    extracted_data = spark.table(source_id)

    logger.info(
        "Data extraction completed",
        extra={
            "attributes": {
                "source": source_id,
            }
        },
    )

    return extracted_data
