"""Job module for portafolios_dynamodb flow.

This job orchestrates the ETL pipeline for processing portfolios from Iceberg
to DynamoDB with CDC support, change detection, restart capability, and
automatic first-run detection.
"""

import time

from pyspark.sql import SparkSession

from quind_demo_ppd_project.flows.portafolios_dynamodb.extract import extract
from quind_demo_ppd_project.flows.portafolios_dynamodb.load import load
from quind_demo_ppd_project.flows.portafolios_dynamodb.transform import transform
from quind_demo_ppd_project.libs.aws.dynamodb.restart_table import restart_table
from quind_demo_ppd_project.libs.iceberg.cdc_with_tagging_strategy import (
    SnapshotManager,
)
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource
from quind_demo_ppd_project.libs.runner.types import Status


def portafolios_dynamodb_job(spark: SparkSession, vars_instance: VarsResource) -> Status:
    """Orchestrate ETL pipeline for portafolios_dynamodb flow.

    This function orchestrates the ETL pipeline with CDC support, change detection,
    restart capability, and automatic first-run detection. It:
    1. Checks for restart configuration and executes restart if needed
    2. Detects first run by checking if snapshot tag exists
    3. Detects changes using snapshot comparison
    4. Extracts data (full or incremental based on first_run)
    5. Transforms data through transformation pipeline
    6. Loads data to DynamoDB
    7. Tags current snapshot for next incremental run

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration loaded from
            config/default.toml. Contains all flow configuration including:
            - Input/output table IDs
            - Component name for restart and tag naming
            - DynamoDB table name and region

    Returns:
        Status object indicating job completion status.
    """
    logger = get_logger(__name__)

    start_time = time.time()
    job_id = f"portafolios_dynamodb_{int(start_time)}"

    component_name = vars_instance.vars.get("component_name", "portafolios_dynamodb")
    input_table_id = vars_instance.vars.input.table_id
    output_table_name = vars_instance.vars.output.table_name
    output_region = vars_instance.vars.output.get("region", "us-east-1")

    tag_name = vars_instance.vars.get(
        "last_snapshot_tag_name",
        f"{component_name}_last_snapshot",
    )

    logger.info(
        "Starting portafolios dynamodb processing job",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "job_name": "portafolios_dynamodb_job",
                "component": component_name,
                "input_table": input_table_id,
                "output_table": output_table_name,
                "tag_name": tag_name,
                "status": "STARTED",
            }
        },
    )

    snapshot_manager = SnapshotManager(spark=spark, table_id=input_table_id)

    restart_flag = getattr(vars_instance.vars, "restart", {}).get(component_name, False)
    if restart_flag:
        logger.info(
            "Restart flag detected, restarting DynamoDB table",
            extra={
                "attributes": {
                    "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                    "job_id": job_id,
                    "step": "restart",
                    "table_name": output_table_name,
                    "region": output_region,
                    "status": "IN_PROGRESS",
                }
            },
        )

        restart_success = restart_table(output_table_name, output_region)
        if not restart_success:
            logger.error(
                "Failed to restart DynamoDB table",
                extra={
                    "attributes": {
                        "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                        "job_id": job_id,
                        "step": "restart",
                        "table_name": output_table_name,
                        "status": "ERROR",
                    }
                },
            )
            return Status(
                status_value="ERROR",
                message="Failed to restart DynamoDB table",
            )

        logger.info(
            "DynamoDB table restarted successfully",
            extra={
                "attributes": {
                    "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                    "job_id": job_id,
                    "step": "restart",
                    "table_name": output_table_name,
                    "status": "DONE",
                }
            },
        )

    first_run = not snapshot_manager.last_snapshot_tag_exists(tag_name)

    logger.info(
        "First run detection completed",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "first_run_detection",
                "first_run": first_run,
                "tag_name": tag_name,
                "status": "DONE",
            }
        },
    )

    if not first_run:
        logger.info(
            "Checking for changes in source table",
            extra={
                "attributes": {
                    "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                    "job_id": job_id,
                    "step": "change_detection",
                    "tag_name": tag_name,
                    "status": "IN_PROGRESS",
                }
            },
        )

        has_changed = snapshot_manager.has_changed(tag_name)

        if not has_changed:
            logger.info(
                "No changes detected in source table, skipping processing",
                extra={
                    "attributes": {
                        "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                        "job_id": job_id,
                        "step": "change_detection",
                        "tag_name": tag_name,
                        "status": "DONE",
                    }
                },
            )
            return Status(
                status_value="OK",
                message="No changes detected, job skipped",
            )

        logger.info(
            "Changes detected in source table, proceeding with processing",
            extra={
                "attributes": {
                    "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                    "job_id": job_id,
                    "step": "change_detection",
                    "tag_name": tag_name,
                    "status": "DONE",
                }
            },
        )

    logger.info(
        "Starting data extraction",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "extract",
                "source_table": input_table_id,
                "first_run": first_run,
                "status": "IN_PROGRESS",
            }
        },
    )

    extracted_data = extract(
        spark=spark,
        vars_instance=vars_instance,
        first_run=first_run,
    )

    logger.info(
        "Data extraction completed",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "extract",
                "source_table": input_table_id,
                "first_run": first_run,
                "status": "DONE",
            }
        },
    )

    logger.info(
        "Starting data transformation",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "transform",
                "status": "IN_PROGRESS",
            }
        },
    )

    transformed_data = transform(
        job_id=job_id,
        spark=spark,
        vars_instance=vars_instance,
        extracted_data=extracted_data,
    )

    logger.info(
        "Data transformation completed",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "transform",
                "status": "DONE",
            }
        },
    )

    logger.info(
        "Starting data load",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "load",
                "target_table": output_table_name,
                "first_run": first_run,
                "status": "IN_PROGRESS",
            }
        },
    )

    load(
        job_id=job_id,
        spark=spark,
        vars_instance=vars_instance,
        transformed_data=transformed_data,
        first_run=first_run,
    )

    logger.info(
        "Data load completed",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "load",
                "target_table": output_table_name,
                "first_run": first_run,
                "status": "DONE",
            }
        },
    )

    logger.info(
        "Tagging current snapshot for next incremental run",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "snapshot_tagging",
                "tag_name": tag_name,
                "status": "IN_PROGRESS",
            }
        },
    )

    snapshot_manager.set_last_snapshot_tag(tag_name)

    logger.info(
        "Snapshot tagged successfully",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "snapshot_tagging",
                "tag_name": tag_name,
                "status": "DONE",
            }
        },
    )

    execution_time_ms = int((time.time() - start_time) * 1000)

    logger.info(
        "Portafolios dynamodb processing job completed successfully",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "status": "DONE",
                "execution_time_ms": execution_time_ms,
                "first_run": first_run,
            }
        },
    )

    return Status(
        status_value="OK", message="Portafolios dynamodb job completed successfully"
    )
