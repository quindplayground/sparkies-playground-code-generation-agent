"""Error data extractors."""

from quind_demo_ppd_project.libs.error_handler.extractors.base import (
    RowDataExtractorInterface,
)
from quind_demo_ppd_project.libs.error_handler.extractors.changelog import (
    SparkChangelogExtractor,
)
from quind_demo_ppd_project.libs.error_handler.extractors.full import SparkFullExtractor


MAP_EXTRACTORS = {"changelog": SparkChangelogExtractor, "full": SparkFullExtractor}

__all__ = [
    "RowDataExtractorInterface",
    "SparkChangelogExtractor",
    "SparkFullExtractor",
    "MAP_EXTRACTORS",
]
