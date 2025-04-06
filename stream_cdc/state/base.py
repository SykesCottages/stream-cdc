from abc import ABC, abstractmethod
from typing import Dict, Optional


class StateManager(ABC):
    @abstractmethod
    def store(
        self,
        datasource_type: str,
        datasource_source: str,
        state_position: Dict[str, str],
    ) -> bool:
        pass

    @abstractmethod
    def read(
        self, datasource_type: str, datasource_source: str
    ) -> Optional[Dict[str, str]]:
        pass
