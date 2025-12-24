"""Job module for portafolios_dynamodb flow.

This module orchestrates the ETL pipeline for processing portafolios data
from Iceberg to DynamoDB:
1. Extract: Reads portafolios data from source Iceberg table
2. Transform: Applies transformations to prepare data for DynamoDB
3. Load: Writes transformed data to DynamoDB
"""

import time

from pyspark.sql import SparkSession

from quind_demo_ppd_project.flows.portafolios_dynamodb.extract import extract
from quind_demo_ppd_project.flows.portafolios_dynamodb.load import load
from quind_demo_ppd_project.flows.portafolios_dynamodb.transform import transform
from quind_demo_ppd_project.libs.logging import get_logger
from quind_demo_ppd_project.libs.resources import VarsResource
from quind_demo_ppd_project.libs.runner.types import Status


def portafolios_dynamodb_job(spark: SparkSession, vars_instance: VarsResource) -> Status:
    """Orchestrate ETL pipeline for portafolios_dynamodb flow.

    This function orchestrates the ETL pipeline by calling extract, transform,
    and load functions in sequence. It provides structured logging and error
    handling throughout the process.

    The actual extraction, transformation, and loading logic is implemented
    in the respective modules (extract.py, transform.py, load.py).

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration loaded from
            config/default.toml. Contains all flow configuration including:
            - Input/output table IDs
            - DynamoDB configuration (table_name, region, item_size_limit)

    Returns:
        Status object indicating job completion status.

    Example:
        ```python
        from quind_demo_ppd_project.libs.resources import get_vars_resource

        # Load configuration
        vars_instance = get_vars_resource(
            env="dev",
            config_paths=["flows/portafolios_dynamodb/config/default.toml"]
        )

        # Execute job
        status = portafolios_dynamodb_job(spark, vars_instance)

        if status.status_value == "OK":
            print("Job completed successfully")
        else:
            print(f"Job failed: {status.message}")
        ```
    """
    logger = get_logger(__name__)

    start_time = time.time()
    job_id = f"portafolios_dynamodb_{int(start_time)}"

    # Get component name from configuration
    component_name = vars_instance.vars.get("component_name", "portafolios_dynamodb")

    logger.info(
        "Starting portafolios dynamodb processing job",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "job_name": "portafolios_dynamodb_job",
                "component": component_name,
                "input_table": vars_instance.vars.input.table_id,
                "output_table": vars_instance.vars.output.table_id,
                "status": "STARTED",
            }
        },
    )

    # EXTRACT
    logger.info(
        "Starting data extraction",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "extract",
                "source_table": vars_instance.vars.input.table_id,
                "status": "IN_PROGRESS",
            }
        },
    )

    extracted_data = extract(
        spark=spark,
        vars_instance=vars_instance,
    )

    logger.info(
        "Data extraction completed",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "extract",
                "source_table": vars_instance.vars.input.table_id,
                "status": "DONE",
            }
        },
    )

    # TRANSFORM
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

    # LOAD
    logger.info(
        "Starting data load",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "load",
                "target_table": vars_instance.vars.output.table_id,
                "status": "IN_PROGRESS",
            }
        },
    )

    load(
        job_id=job_id,
        spark=spark,
        vars_instance=vars_instance,
        transformed_data=transformed_data,
    )

    logger.info(
        "Data load completed",
        extra={
            "attributes": {
                "operation": "EXECUTE_PORTAFOLIOS_DYNAMODB_JOB",
                "job_id": job_id,
                "step": "load",
                "target_table": vars_instance.vars.output.table_id,
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
            }
        },
    )

    return Status(
        status_value="OK", message="Portafolios dynamodb job completed successfully"
    )
