from stream_cdc.state.base import StateManager
from stream_cdc.state.factory import StateManagerFactory
from stream_cdc.state.dynamodb import Dynamodb

# Register Dynamodb as state manager with the factory
StateManagerFactory.register_state_manager("dynamodb", Dynamodb)

__all__ = ["StateManager", "StateManagerFactory", "Dynamodb"]
