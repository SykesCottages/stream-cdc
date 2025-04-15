from typing import Protocol, Dict, Any
from stream_cdc.utils.serializer import Serializer


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
        return self.serializer.serialize(event)
