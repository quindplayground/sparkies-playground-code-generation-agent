"""Job module for portafolios_dynamodb flow.

This job orchestrates the ETL pipeline for processing portafolios data:
1. Extract: Reads portafolios data from source Iceberg table
2. Transform: Applies transformations (filter active, build keys, aggregate products, format timestamp)
3. Load: Writes transformed data to DynamoDB table

The flow processes portafolios from Iceberg to DynamoDB, applying business logic
to filter active records, build DynamoDB keys, aggregate products, and format data
for DynamoDB storage.
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
    """Orchestrate ETL pipeline for portafolios DynamoDB flow.

    This function orchestrates the ETL pipeline by calling extract, transform,
    and load functions in sequence. It provides structured logging and error
    handling throughout the process.

    The pipeline:
    1. Extracts portafolios data from source Iceberg table
    2. Transforms data (filters active records, builds DynamoDB keys, aggregates products, formats timestamp)
    3. Loads transformed data to DynamoDB table using merge strategy

    Args:
        spark: SparkSession for data processing.
        vars_instance: VarsResource instance with configuration loaded from
            config/default.toml. Contains all flow configuration including:
            - vars_instance.vars.input.table_id: Source Iceberg table identifier
            - vars_instance.vars.output.table_name: DynamoDB table name
            - vars_instance.vars.output.region: AWS region for DynamoDB
            - vars_instance.vars.output.item_size_limit: Item size limit in bytes

    Returns:
        Status object indicating job completion status with message.

    Raises:
        AnalysisException: If source table does not exist or cannot be accessed.
        KeyError: If required DynamoDB configuration is missing.
        SparkException: If there is an error during data processing.
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
