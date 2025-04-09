from typing import List

from stream_cdc.filters.base import FilterChain, FilterLike


class FilterFactory:
    """Factory for creating filter components.

    This factory provides methods for creating various filter components,
    facilitating the construction of filter chains and individual filters
    based on configuration or runtime needs.
    """

    @staticmethod
    def create_filter_chain(filters: List[FilterLike]) -> FilterChain:
        """Create a new filter chain with the provided filters.

        Args:
            filters: A list of filters to include in the chain. Filters will be
                    applied in the order they appear in the list.

        Returns:
            A configured FilterChain containing the provided filters.
        """
        return FilterChain(filters)
