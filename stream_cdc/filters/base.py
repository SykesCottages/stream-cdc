from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol
import logging

logger = logging.getLogger(__name__)


# Creates an alias for message type
Message = Dict[str,Any]

class FilterException(Exception):
    """Exception raised for errors in filter operations.

    This exception is used to signal errors that occur during filtering operations,
    allowing for consistent error handling across the filters module.
    """
    pass


class FilterLike(Protocol):
    """Protocol for objects with filter method compatibility.

    This protocol defines the interface for anything that behaves like a filter,
    which makes it compatible with both actual MessageFilter implementations
    and test mocks that implement the same interface.
    """

    def filter(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Filter a message according to implementation-specific rules."""
        ...


class MessageFilter(ABC):
    """Abstract base class for all message filters.

    This class defines the interface for message filters. All concrete filter
    implementations should inherit from this class and implement the filter method.
    """

    @abstractmethod
    def filter(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Apply filtering logic to a message.

        Args:
            message: The message to be filtered.

        Returns:
            The filtered message.

        Raises:
            FilterException: If an error occurs during filtering.
        """
        pass


class FilterChain:
    """A chain of filters that can be applied sequentially to a message.

    FilterChain allows multiple filters to be applied in sequence, where the output
    of one filter becomes the input to the next. This enables modular and composable
    filtering logic.
    """

    def __init__(self, filters: Optional[List[FilterLike]] = None):
        """Initialize a new filter chain.

        Args:
            filters: A list of filters to be applied in sequence. If None, an empty
                    list will be used.
        """
        self.filters = filters or []

    def add_filter(self, message_filter: FilterLike) -> None:
        """Add a filter to the end of the chain.

        Args:
            message_filter: The filter to add to the chain.
        """
        self.filters.append(message_filter)

    def apply(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all filters in the chain to the message.

        The filters are applied in the order they were added to the chain.
        Each filter receives the output of the previous filter.

        Args:
            message: The message to filter.

        Returns:
            The filtered message after applying all filters in the chain.
        """
        filtered_message = message
        for message_filter in self.filters:
            filtered_message = message_filter.filter(filtered_message)
        return filtered_message

