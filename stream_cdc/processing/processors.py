from typing import Protocol, Dict, Any, List

from stream_cdc.utils.serializer import Serializer
from stream_cdc.filters.base import FilterLike
from stream_cdc.filters.factory import FilterFactory
from stream_cdc.utils.logger import logger


class EventProcessor(Protocol):
    """Protocol defining an event processor component."""

    def process(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single event and return the processed result."""
        ...


class DefaultEventProcessor:
    """Default implementation of event processing logic."""

    def __init__(self):
        self.serializer = Serializer()

    def process(self, event: dict) -> dict:
        """Process a single event by serializing it."""

        logger.debug(f"start processing event: {event}")
        # Add any filters here in the order they should be applied.
        filters: List[FilterLike] = []

        serealized_event = self.serializer.serialize(event)

        chain = FilterFactory.create_filter_chain(filters)
        result = chain.apply(serealized_event)

        return result
