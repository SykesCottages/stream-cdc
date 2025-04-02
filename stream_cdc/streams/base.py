from abc import ABC, abstractmethod
from typing import List, Any


class Stream(ABC):
    """
    Base abstract class for all stream implementations.

    This abstract class defines the interface that all stream implementations must
    adhere to. Stream implementations are responsible for sending messages to
    various destinations (e.g., AWS SQS, Kafka, etc.).
    """

    @abstractmethod
    def send(self, messages: List[Any]) -> None:
        """
        Send messages to the stream destination.

        Args:
            messages (List[Any]): The messages to send.

        Raises:
            StreamError: If the send operation fails.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close any open connections or resources.

        This method should perform any necessary cleanup, such as closing connections
        or releasing resources held by the stream implementation.

        Raises:
            StreamError: If the close operation fails.
        """
        pass
