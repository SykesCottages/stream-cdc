from abc import ABC, abstractmethod


class StateManager(ABC):
    @abstractmethod
    def store(self):
        pass

    @abstractmethod
    def read(self):
        pass
