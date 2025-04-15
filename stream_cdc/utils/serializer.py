from typing import Any, Dict
import copy
from stream_cdc.utils.logger import logger

class Serializer:
    """
    Utility class for serializing data to JSON-compatible formats.
    """
    def serialize(self, data: Any) -> Dict:
        """
        Serialize data to a JSON-compatible format.

        First converts all non-JSON-serializable types (like bytes and datetime)
        to string representations, then attempts to serialize.

        Args:
            data (Any): The data to serialize.
        Returns:
            Dict: The serialized data as a JSON-compatible structure.
        """
        try:
            processed_data = self._make_json_compatible(copy.deepcopy(data))
        except Exception as e:
            logger.debug(f"Serialization exception: {e}, converting entire object to string")

        return processed_data

    def _make_json_compatible(self, obj: Any) -> Any:
        """
        Recursively convert an object to ensure JSON compatibility.
        """
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                # Convert bytes keys to strings
                key = k.decode('utf-8') if isinstance(k, bytes) else str(k)
                result[key] = self._make_json_compatible(v)
            return result
        elif isinstance(obj, list):
            return [self._make_json_compatible(item) for item in obj]
        elif isinstance(obj, tuple):
            return [self._make_json_compatible(item) for item in obj]
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return str(obj)
        # Return primitive types as-is
        elif obj is None or isinstance(obj, (str, int, float, bool)):
            return obj
        # Convert anything else to string
        else:
            return str(obj)
