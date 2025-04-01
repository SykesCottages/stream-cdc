from abc import ABC, abstractmethod
from typing import Generator, Any, Dict

class DataSource(ABC):
    """Base abstract class for all data source implementations."""

    @abstractmethod
    def connect(self) -> None:
        """Connect to the data source."""
        pass

    @abstractmethod
    def listen(self) -> Generator[Dict[str, Any], None, None]:
        """Listen for changes from the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the data source."""
        pass
