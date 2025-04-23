from abc import ABC, abstractmethod
from stream_cdc.position.position import Position


class StateManager(ABC):
    @abstractmethod
    def store(
        self, datasource_type: str, datasource_source: str, state_position: Position
    ) -> bool:
        pass

    @abstractmethod
    def read(self, datasource_type: str, datasource_source: str) -> Position:
        pass
