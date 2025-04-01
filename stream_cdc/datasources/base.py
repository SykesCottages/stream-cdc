from abc import ABC, abstractmethod
from typing import Generator, Dict, Any


class DataSource(ABC):
    """
    Base abstract class for all data source implementations.

    This abstract class defines the interface that all data source implementations must
    adhere to. Data sources are responsible for connecting to and listening for changes
    from various sources (e.g., MySQL, PostgreSQL, MongoDB, etc.).
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Connect to the data source.

        This method should establish a connection to the data source and prepare
        it for listening to changes.

        Raises:
            DataSourceError: If connection fails.
        """
        pass

    @abstractmethod
    def listen(self) -> Generator[Dict[str, Any], None, None]:
        """
        Listen for changes from the data source.

        This method should yield events representing changes from the data source.

        Yields:
            Dict[str, Any]: Events representing changes from the data source.

        Raises:
            DataSourceError: If listening fails.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnect from the data source.

        This method should close any open connections or resources.

        Raises:
            DataSourceError: If disconnection fails.
        """
        pass

