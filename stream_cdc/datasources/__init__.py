from stream_cdc.datasources.base import DataSource
from stream_cdc.datasources.factory import DataSourceFactory
from stream_cdc.datasources.mysql import MySQLDataSource

# Register the MySQL data source with the factory
DataSourceFactory.register_datasource("mysql", MySQLDataSource)

__all__ = ["DataSource", "DataSourceFactory", "MySQLDataSource"]
