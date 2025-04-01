from typing import Dict, ClassVar, Type
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import UnsupportedTypeError
from stream_cdc.streams.base import Stream


class StreamFactory:
    """Factory for creating Stream implementations."""

    REGISTRY: ClassVar[Dict[str, Type[Stream]]] = {}

    @classmethod
    def register_stream(cls, name: str, datasource_class: Type[Stream]) -> None:
        """
        Register a data source implementation.

        Args:
            name: The name to register the data source under
            datasource_class: The data source class to register
        """
        cls.REGISTRY[name.lower()] = datasource_class

    @classmethod
    def create(cls, stream_type: str, **kwargs) -> Stream:
        """
        Create a Stream implementation based on requested type.

        Args:
            stream_type: The type of stream to create
            **kwargs: Configuration parameters to pass to the stream implementation

        Returns:
            An initialized Stream implementation

        Raises:
            UnsupportedTypeError: If the requested stream type is not supported
        """
        normalized_type = stream_type.lower()
        logger.debug(f"Creating stream of type: {normalized_type}")

        if normalized_type not in cls.REGISTRY:
            supported = list(cls.REGISTRY.keys())
            logger.error(f"Unsupported stream type: {stream_type}. Supported types: {supported}")
            raise UnsupportedTypeError(f"Unsupported stream type: {stream_type}. Supported types: {supported}")

        stream_class = cls.REGISTRY[normalized_type]
        return stream_class(**kwargs)

