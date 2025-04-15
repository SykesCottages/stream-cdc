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

    @abstractmethod
    def get_position(self) -> Dict[str, str]:
        """
        Get the current position of the data source.

        This method should return a dictionary representing the current position
        in a format that can be used to resume streaming from this point.

        Returns:
            Dict[str, str]: A dictionary with position information.
        """
        pass

    @abstractmethod
    def set_position(self, position: Dict[str, str]) -> None:
        """
        Set the starting position for streaming.

        This method should configure the data source to start streaming
        from the specified position.

        Args:
            position (Dict[str, str]): The position information.
        """
        pass

    @abstractmethod
    def get_source_type(self) -> str:
        """
        Get the type identifier for this datasource.

        Returns:
            str: A string identifier for the type of this datasource.
        """
        pass

    @abstractmethod
    def get_source_id(self) -> str:
        """
        Get the unique identifier for this datasource instance.

        Returns:
            str: A string uniquely identifying this datasource instance.
        """
        pass
