"""Base writer for error data."""

from abc import ABC, abstractmethod
from quind_demo_ppd_project.libs.error_handler.models import ErrorData


class ErrorWriterInterface(ABC):
    """Abstract base class for writing error data."""

    @abstractmethod
    def write_error(self, error_data: ErrorData) -> None:
        """Write the given error data.

        Args:
            error_data: The error data to write.
        """
        pass
