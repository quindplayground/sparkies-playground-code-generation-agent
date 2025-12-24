"""Error data writers."""

from quind_demo_ppd_project.libs.error_handler.writers.base import ErrorWriterInterface
from quind_demo_ppd_project.libs.error_handler.writers.spark_csv import (
    SparkCsvErrorWriter,
)

MAP_WRITERS = {"spark_csv": SparkCsvErrorWriter}

__all__ = ["ErrorWriterInterface", "SparkCsvErrorWriter", "MAP_WRITERS"]
