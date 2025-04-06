from typing import Dict, ClassVar, Type
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import UnsupportedTypeError
from stream_cdc.streams.base import Stream


class StreamFactory:
    """
    Factory for creating Stream implementations.

    This class provides a registry-based factory pattern for creating instances
    of Stream implementations based on a specified type. New stream types can be
    registered with the factory to make them available for creation.
    """

    REGISTRY: ClassVar[Dict[str, Type[Stream]]] = {}

    @classmethod
    def register_stream(cls, name: str, datasource_class: Type[Stream]) -> None:
        """
        Register a stream implementation.

        Args:
            name (str): The name to register the stream under.
            datasource_class (Type[Stream]): The stream class to register.
        """
        cls.REGISTRY[name.lower()] = datasource_class

    @classmethod
    def create(cls, stream_type: str) -> Stream:
        """
        Create a Stream implementation based on requested type.

        Args:
            stream_type (str): The type of stream to create.

        Returns:
            Stream: An initialized Stream implementation.

        Raises:
            UnsupportedTypeError: If the requested stream type is not supported.
        """
        normalized_type = stream_type.lower()
        logger.debug(f"Creating stream of type: {normalized_type}")

        if normalized_type not in cls.REGISTRY:
            supported = list(cls.REGISTRY.keys())
            logger.error(
                f"Unsupported stream type: {stream_type}. Supported types: {supported}"
            )
            raise UnsupportedTypeError(
                f"Unsupported stream type: {stream_type}. Supported types: {supported}"
            )

        stream_class = cls.REGISTRY[normalized_type]
        return stream_class()
