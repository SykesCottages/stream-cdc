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


# class Serializer:
#     """
#     Utility class for serializing data to JSON-compatible formats.
#     """
#     def serialize(self, data: Any) -> Dict:
#         """
#         Serialize data to a JSON-compatible format.
#
#         First converts all non-JSON-serializable types (like bytes and datetime)
#         to string representations, then attempts to serialize.
#
#         Args:
#             data (Any): The data to serialize.
#         Returns:
#             Dict: The serialized data as a JSON-compatible structure.
#         """
#         try:
#             processed_data = self._make_json_compatible(copy.deepcopy(data))
#         except Exception as e:
#             logger.debug(f"Serialization exception: {e}, converting entire object to string")
#
#         return processed_data
#
#     def _make_json_compatible(self, obj: Any) -> Any:
#         """
#         Recursively convert an object to ensure JSON compatibility.
#         """
#         if isinstance(obj, dict):
#             result = {}
#             for k, v in obj.items():
#                 # Convert bytes keys to strings
#                 key = k.decode('utf-8') if isinstance(k, bytes) else str(k)
#                 result[key] = self._make_json_compatible(v)
#             return result
#         elif isinstance(obj, list):
#             return [self._make_json_compatible(item) for item in obj]
#         elif isinstance(obj, tuple):
#             return [self._make_json_compatible(item) for item in obj]
#         elif isinstance(obj, bytes):
#             try:
#                 return obj.decode('utf-8')
#             except UnicodeDecodeError:
#                 return str(obj)
#         # Return primitive types as-is
#         elif obj is None or isinstance(obj, (str, int, float, bool)):
#             return obj
#         # Convert anything else to string
#         else:
#             return str(obj)
