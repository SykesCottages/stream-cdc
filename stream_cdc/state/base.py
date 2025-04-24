from abc import ABC, abstractmethod


class StateManager(ABC):
    @abstractmethod
    def store(
        self, datasource_type: str, datasource_source: str, state_position: str
    ) -> bool:
        pass

    @abstractmethod
    def read(self, datasource_type: str, datasource_source: str) -> str:
        pass
