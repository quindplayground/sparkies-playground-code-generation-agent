"""Resources module for Spark and configuration management."""

from quind_demo_ppd_project.libs.resources.vars.resource import (
    VarsResource,
    get_vars_resource,
)
from quind_demo_ppd_project.libs.resources.spark_resource import SparkResource

__all__ = ["VarsResource", "SparkResource", "get_vars_resource"]
