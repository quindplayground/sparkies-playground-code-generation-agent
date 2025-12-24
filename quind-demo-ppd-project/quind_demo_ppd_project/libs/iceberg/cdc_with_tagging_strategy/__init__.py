"""CDC (Change Data Capture) with tagging strategies for Iceberg tables."""

from quind_demo_ppd_project.libs.iceberg.cdc_with_tagging_strategy.changelog_manager import (
    ChangelogManager,
)
from quind_demo_ppd_project.libs.iceberg.cdc_with_tagging_strategy.snapshot_manager import (
    SnapshotManager,
)

__all__ = ["ChangelogManager", "SnapshotManager"]
