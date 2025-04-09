from unittest.mock import MagicMock

from stream_cdc.filters.base import MessageFilter, FilterChain
from stream_cdc.filters.factory import FilterFactory


class TestFilterFactory:
    """Test suite for the FilterFactory class."""

    def test_create_filter_chain(self):
        """Test that create_filter_chain correctly creates a FilterChain with
        the provided filters."""

        # Create mock filters
        mock_filter1 = MagicMock(spec=MessageFilter)
        mock_filter2 = MagicMock(spec=MessageFilter)

        filters = [mock_filter1, mock_filter2]

        # Create a filter chain using the factory
        chain = FilterFactory.create_filter_chain(filters)  # type: ignore

        # Verify the chain is of the correct type
        assert isinstance(chain, FilterChain)

        # Verify the chain contains the provided filters
        assert chain.filters == filters

