"""Extract module for portafolios_dynamodb flow.

This module handles incremental data extraction from Iceberg source table
using CDC (Change Data Capture) with snapshot comparison strategy.
"""

from pyspark.sql import DataFrame, SparkSession

from quind_demo_ppd_project.libs.error_handler import handle_errors
from quind_demo_ppd_project.libs.iceberg.cdc_with_tagging_strategy import (
    ChangelogManager,
)
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource

logger = get_logger(__name__)


@handle_errors
def extract(
    spark: SparkSession,
    vars_instance: VarsResource,
) -> DataFrame:
    """Extract data from Iceberg source table using incremental CDC strategy.

    This function implements incremental extraction using CDC with Iceberg
    snapshot comparison strategy (iceberg_sp). On first execution (when no
    snapshot tag exists), it extracts the complete table. On subsequent
    executions, it extracts only changed records since the last snapshot.

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.input.table_id: Source Iceberg table identifier
            - vars_instance.vars.get("last_snapshot_tag_name"): Tag name for
              snapshot tracking (configured in default.toml)

    Returns:
        DataFrame containing extracted data from source. On first execution,
        returns complete table. On subsequent executions, returns only changed
        records (inserts, updates, deletes) since last snapshot.

    Example:
        ```python
        extracted_data = extract(spark=spark, vars_instance=vars_instance)
        ```
    """
    source_table_id = vars_instance.vars.input.table_id
    tag_name = vars_instance.vars.get("last_snapshot_tag_name")

    logger.info(
        "Extracting data from Iceberg source table using CDC",
        extra={
            "attributes": {
                "source_table": source_table_id,
                "tag_name": tag_name,
                "cdc_strategy": "iceberg_sp",
            }
        },
    )

    changelog_manager = ChangelogManager(
        spark=spark,
        table_id=source_table_id,
        tag_name=tag_name,
    )

    changelog_df = changelog_manager.get_changelog_table(
        changelog_strategy="iceberg_sp"
    )

    logger.info(
        "Data extraction completed",
        extra={
            "attributes": {
                "source_table": source_table_id,
                "tag_name": tag_name,
                "cdc_strategy": "iceberg_sp",
            }
        },
    )

    return changelog_df
