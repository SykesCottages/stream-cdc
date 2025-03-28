from typing import Any
import json
from logger import logger


class Serializer:
    def serialize(self, data: Any) -> Any:
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

