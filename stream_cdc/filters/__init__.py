"""Filter module for processing messages in the CDC stream.

This module provides a framework for filtering messages in a modular and composable way.
Filters can be chained together to create complex filtering logic while keeping
individual filters simple and focused on a single responsibility.

Key components:
- MessageFilter: Abstract base class for all message filters
- FilterChain: Class for chaining multiple filters together
- FilterFactory: Factory for creating filter components
- FilterException: Exception raised for errors during filtering
"""

from stream_cdc.filters.base import (
    Message,
    MessageFilter,
    FilterChain,
    FilterException,
)
from stream_cdc.filters.factory import FilterFactory

__all__ = [
    "Message",
    "MessageFilter",
    "FilterChain",
    "FilterFactory",
    "FilterException",
]

