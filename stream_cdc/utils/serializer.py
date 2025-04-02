from typing import Any
import json
from stream_cdc.utils.logger import logger


class Serializer:
    """
    Utility class for serializing data to JSON-compatible formats.

    This class handles the conversion of various data types to formats that can be
    safely transmitted over data streams, with fallback to string conversion for
    complex objects that cannot be directly serialized to JSON.
    """

    def serialize(self, data: Any) -> Any:
        """
        Serialize data to a JSON-compatible format.

        Attempts to convert the data to JSON and then parse it back to ensure it's
        properly serialized. If this fails, converts the entire object to a string.

        Args:
            data (Any): The data to serialize.

        Returns:
            Any: The serialized data, either as a JSON-compatible structure or a string.
        """
        try:
            return json.loads(json.dumps(data, default=str))
        except Exception as e:
            # Probably not the best approach but transforming anything into a
            # string makes it easier to push raw data to a stream
            # Open to suggestions
            logger.debug(
                f"Serialization exception: {e}, converting entire object to string"
            )
            return str(data)
