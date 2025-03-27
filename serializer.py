from datetime import datetime
from typing import Any

class Serializer:
    def serialize(self, data: Any) -> Any:
        if isinstance(data, dict):
            return {key: self.serialize(value)
                    for key, value in data.items()}
        if isinstance(data, datetime):
            return data.isoformat()
        return data

