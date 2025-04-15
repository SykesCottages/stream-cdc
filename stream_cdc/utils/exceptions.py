class StreamCDCError(Exception):
    """Base exception for all Stream CDC related errors."""

    pass


class ConfigurationError(StreamCDCError):
    """Raised when there is an issue with configuration settings."""

    pass


class UnsupportedTypeError(StreamCDCError):
    """Raised when an unsupported type is requested from a factory."""

    pass


class DataSourceError(StreamCDCError):
    """Raised when there is an issue with a data source operation."""

    pass


class ConnectionError(StreamCDCError):
    """Raised when there is an issue connecting to a resource."""

    pass


class ReceivingError(StreamCDCError):
    """Raised when there is an issue receiving data from a source."""

    pass


class StreamError(StreamCDCError):
    """Raised when there is an issue with a stream operation."""

    pass


class StateError(StreamCDCError):
    """Raised when there is an issue with state management."""

    pass


class ProcessingError(StreamCDCError):
    """Raised when there is an issue with data processing."""

    pass


class SerializationError(StreamCDCError):
    """Raised when there is an issue with data serialization."""

    pass
