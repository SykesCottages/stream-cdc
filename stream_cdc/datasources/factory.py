from typing import Dict, ClassVar, Type
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import UnsupportedTypeError
from stream_cdc.datasources.base import DataSource


class DataSourceFactory:
    """
    Factory for creating DataSource implementations.

    This class provides a registry-based factory pattern for creating instances
    of DataSource implementations based on a specified type. New data source types
    can be registered with the factory to make them available for creation.
    """

    REGISTRY: ClassVar[Dict[str, Type[DataSource]]] = {}

    @classmethod
    def register_datasource(
        cls, name: str, datasource_class: Type[DataSource]
    ) -> None:
        """
        Register a data source implementation.

        Args:
            name (str): The name to register the data source under.
            datasource_class (Type[DataSource]): The data source class to register.
        """
        cls.REGISTRY[name.lower()] = datasource_class

    @classmethod
    def create(cls, datasource_type: str, **kwargs) -> DataSource:
        """
        Create a DataSource implementation based on requested type.

        Args:
            datasource_type (str): The type of data source to create.
            **kwargs: Configuration parameters to pass to the data source
            implementation.

        Returns:
            DataSource: An initialized DataSource implementation.

        Raises:
            UnsupportedTypeError: If the requested data source type is not supported.
        """
        normalized_type = datasource_type.lower()
        logger.debug(f"Creating data source of type: {normalized_type}")

        if normalized_type not in cls.REGISTRY:
            supported = list(cls.REGISTRY.keys())
            logger.error(
                f"Unsupported data source type: {datasource_type}. Supported types: "
                f"{supported}"
            )
            raise UnsupportedTypeError(
                f"Unsupported data source type: {datasource_type}. Supported types: "
                f"{supported}"
            )

        datasource_class = cls.REGISTRY[normalized_type]
        return datasource_class(**kwargs)
