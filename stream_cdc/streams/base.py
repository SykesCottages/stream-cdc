from abc import ABC, abstractmethod
from typing import List, Any

class Stream(ABC):
    """Base abstract class for all stream implementations."""

    @abstractmethod
    def send(self, messages: List[Any]) -> None:
        """Send messages to the stream destination."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close any open connections or resources."""
        pass

