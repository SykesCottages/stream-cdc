from typing import Dict, ClassVar, Type, Any

from stream_cdc.datasources.base import DataSource
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import UnsupportedTypeError


class DataSourceFactory:
    """Factory for creating DataSource implementations."""

    # Registry of available data source types
    # This will be populated by register_datasource
    REGISTRY: ClassVar[Dict[str, Type[DataSource]]] = {}

    @classmethod
    def register_datasource(cls, name: str, datasource_class: Type[DataSource]) -> None:
        """
        Register a data source implementation.

        Args:
            name: The name to register the data source under
            datasource_class: The data source class to register
        """
        cls.REGISTRY[name.lower()] = datasource_class

    @classmethod
    def create(cls, db_type: str, **kwargs) -> DataSource:
        """
        Create a DataSource implementation based on requested type.

        Args:
            db_type: The type of data source to create
            **kwargs: Configuration parameters to pass to the data source implementation

        Returns:
            An initialized DataSource implementation

        Raises:
            UnsupportedTypeError: If the requested data source type is not supported
        """
        normalized_type = db_type.lower()
        logger.debug(f"Creating data source of type: {normalized_type}")

        if normalized_type not in cls.REGISTRY:
            supported = list(cls.REGISTRY.keys())
            error_msg = f"Unsupported database type: {db_type}. Supported types: {supported}"
            logger.error(error_msg)
            raise UnsupportedTypeError(error_msg)

        datasource_class = cls.REGISTRY[normalized_type]
        return datasource_class(**kwargs)

