"""Extract module for portafolios_dynamodb flow.

This module handles data extraction from Iceberg source table.
Supports both full extraction (first run) and incremental extraction
using changelog strategy.
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
    """Extract data from Iceberg source table.

    Extracts data from the portafolios Iceberg table. Supports two modes:
    - Full extraction: Reads the complete table (first_run=True or default)
    - Incremental extraction: Reads only changes since last snapshot (first_run=False)

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration.
            - vars_instance.vars.input.table_id: Source table identifier
            - vars_instance.vars.input.error_path: Path for error logs
        **kwargs: Optional parameters.
            - first_run: Boolean indicating if this is the first run.
                If True or not provided, extracts full table.
                If False, extracts incremental changes using changelog.

    Returns:
        DataFrame containing extracted data from source table.

    Raises:
        Exception: If extraction fails or table is not found.
    """
    source_table_id = vars_instance.vars.input.table_id
    first_run = kwargs.get("first_run", True)

    logger.info(
        "Starting data extraction",
        extra={
            "attributes": {
                "source_table": source_table_id,
                "extraction_mode": "full" if first_run else "incremental",
                "first_run": first_run,
            }
        },
    )

    if first_run:
        logger.info(
            "First run detected, extracting full table",
            extra={
                "attributes": {
                    "source_table": source_table_id,
                }
            },
        )
        result = spark.table(source_table_id)
    else:
        logger.info(
            "Incremental run detected, extracting changelog",
            extra={
                "attributes": {
                    "source_table": source_table_id,
                }
            },
        )
        from quind_demo_ppd_project.libs.iceberg.cdc_with_tagging_strategy import (
            ChangelogManager,
        )

        tag_name = vars_instance.vars.get(
            "last_snapshot_tag_name",
            f"{vars_instance.vars.get('component_name', 'portafolios_dynamodb')}_last_snapshot",
        )

        changelog_manager = ChangelogManager(
            spark=spark,
            table_id=source_table_id,
            tag_name=tag_name,
        )

        result = changelog_manager.get_changelog_table(changelog_strategy="iceberg_sp")

    logger.info(
        "Data extraction completed",
        extra={
            "attributes": {
                "source_table": source_table_id,
                "extraction_mode": "full" if first_run else "incremental",
                "row_count": result.count(),
            }
        },
    )

    return result
