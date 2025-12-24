"""Extract module for portafolios_dynamodb flow.

This module handles data extraction from the source Iceberg table containing
portafolios data. The extraction implements a full load strategy, reading all
data from the source table.
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
) -> DataFrame:
    """Extract data from source Iceberg table.

    This function extracts all data from the source Iceberg table containing
    portafolios data. The extraction uses a full load strategy, reading the
    entire table on each execution.

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.input.table_id: Source table identifier
              (format: catalog.database.table_name)

    Returns:
        DataFrame containing extracted portafolios data from source table.

    Example:
        ```python
        extracted_data = extract(spark=spark, vars_instance=vars_instance)
        ```
    """
    source_table_id = vars_instance.vars.input.table_id

    logger.info(
        "Extracting data from source table",
        extra={"attributes": {"source_table": source_table_id}},
    )

    source_data = spark.table(source_table_id)

    logger.info(
        "Data extraction completed",
        extra={"attributes": {"source_table": source_table_id}},
    )

    return source_data
