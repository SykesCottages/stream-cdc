from typing import ClassVar, Dict, Type
from stream_cdc.state.base import StateManager
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import UnsupportedTypeError


class StateManagerFactory:
    """
    Factory for creating a StateManager.
    """

    REGISTRY: ClassVar[Dict[str, Type[StateManager]]] = {}


    @classmethod
    def register_state_manager(
            cls,
            name: str,
            state_manager_class: Type[StateManager]
        ) -> None:

        cls.REGISTRY[name.lower()] = state_manager_class


    @classmethod
    def create(cls, state_manager_type: str, **kwargs) -> StateManager:
        """
        Create a State Manager implementation based on requested type.

        Args:
            state_manager_type (str): The type of state manager.
            **kwargs: Configuration parameter to pass in.

        Returns:
            StateManager: An initialized StateManager implementation.

        """
        normalized_type = state_manager_type.lower()
        logger.debug(f"Creating state manager of type: {normalized_type}")

        if normalized_type not in cls.REGISTRY:
            supported = list(cls.REGISTRY.keys())
            logger.error(
            f"Unsupported state manager type: {state_manager_type}. "
            f"Supported types: {supported}")
            raise UnsupportedTypeError(f"Unsupported state manager type: "
                                       f"{state_manager_type}.")

        state_manager_class = cls.REGISTRY[normalized_type]
        return state_manager_class(**kwargs)
