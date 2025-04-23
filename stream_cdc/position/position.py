from typing import Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class Position(Protocol):
    """
    Protocol defining the interface for position tracking.

    Position objects represent a point-in-time location in a data stream
    that can be used to resume processing from that point.
    """

    def to_dict(self) -> Dict[str, str]:
        """
        Convert the position to a dictionary representation for storage.

        Returns:
            Dict[str, str]: Dictionary representation of the position
        """
        ...

    def from_dict(self, position_data: Dict[str, str]) -> None:
        """
        Update this position object from a dictionary representation.

        Args:
            position_data: Dictionary representation of position data
        """
        ...

    def is_valid(self) -> bool:
        """
        Check if this position is valid and can be used.

        Returns:
            bool: True if position is valid, False otherwise
        """
        ...

    def __str__(self) -> str:
        """
        Get a string representation of this position.

        Returns:
            str: Human-readable representation of the position
        """
        ...
