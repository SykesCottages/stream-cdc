class ConfigurationError(Exception):
    """
    Exception raised when there is an issue with the configuration.

    This exception is raised when required configuration parameters are missing,
    have invalid values, or when there is a conflict in the configuration.
    """
    pass


class StreamError(Exception):
    """
    Exception raised when there is an issue with a stream operation.

    This exception is raised when a stream operation (such as sending a message)
    fails due to connection issues, message size limitations, or other stream-related
    problems.
    """
    pass


class DataSourceError(Exception):
    """
    Exception raised when there is an issue with a data source.

    This exception is raised when a data source operation (such as connecting,
    querying, or reading data) fails due to connection issues, invalid credentials,
    or other data source-related problems.
    """
    pass


class ProcessingError(Exception):
    """
    Exception raised when there is an issue with data processing.

    This exception is raised when data processing operations fail due to
    invalid data formats, transformation errors, or other processing-related
    issues.
    """
    pass


class UnsupportedTypeError(Exception):
    """
    Exception raised when an unsupported type is requested.

    This exception is raised when the application attempts to create an instance
    of a type that is not registered or supported, such as an unknown stream or
    data source type.
    """
    pass

