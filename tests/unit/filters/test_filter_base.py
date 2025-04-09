from unittest.mock import MagicMock

from stream_cdc.filters.base import MessageFilter, FilterChain, FilterLike
from stream_cdc.filters.factory import FilterFactory


class TestFilterChain:
    """Test suite for the FilterChain class."""

    def test_empty_chain_returns_original_message(self):
        """Test that an empty filter chain returns the original message unchanged."""
        chain = FilterChain()
        message = {"test": "value"}

        result = chain.apply(message)

        assert result == message

    def test_add_filter_appends_to_chain(self):
        """Test that add_filter correctly appends a filter to the chain."""
        chain = FilterChain()
        mock_filter = MagicMock(spec=MessageFilter)

        chain.add_filter(mock_filter)

        assert len(chain.filters) == 1
        assert chain.filters[0] == mock_filter

    def test_apply_calls_filters_in_order(self):
        """Test that filters are applied in the correct order in the chain."""
        filter1 = MagicMock(spec=MessageFilter)
        filter1.filter.return_value = {"test": "modified1"}

        filter2 = MagicMock(spec=MessageFilter)
        filter2.filter.return_value = {"test": "modified2"}

        chain = FilterChain([filter1, filter2])
        message = {"test": "original"}

        result = chain.apply(message)

        # Verify filter1 was called with the original message
        filter1.filter.assert_called_once_with(message)

        # Verify filter2 was called with the output from filter1
        filter2.filter.assert_called_once_with({"test": "modified1"})

        # Verify the final result is the output from filter2
        assert result == {"test": "modified2"}

    def test_chain_with_non_messagefilter_objects(self):
        """Test that FilterChain works with objects that implement filter method but aren't MessageFilter."""

        class CustomFilter:
            def filter(self, message):
                message["custom"] = "modified"
                return message

        custom_filter = CustomFilter()
        chain = FilterChain([custom_filter])
        message = {"test": "original"}

        result = chain.apply(message)

        assert result == {"test": "original", "custom": "modified"}


class TestFilterFactory:
    """Test suite for the FilterFactory class."""

    def test_create_filter_chain_with_messagefilters(self):
        """Test that create_filter_chain works with MessageFilter objects."""

        # Create concrete MessageFilter implementations
        class TestFilter(MessageFilter):
            def filter(self, message):
                return {"processed": True}

        filters = [TestFilter(), TestFilter()]

        chain = FilterFactory.create_filter_chain(filters)

        assert isinstance(chain, FilterChain)
        assert len(chain.filters) == 2
        assert chain.filters == filters

    def test_create_filter_chain_with_mocks(self):
        """Test that create_filter_chain works with mock objects that have filter method."""
        mock_filter1 = MagicMock(spec=MessageFilter)
        mock_filter1.filter.return_value = {"result": "mock1"}

        mock_filter2 = MagicMock(spec=MessageFilter)
        mock_filter2.filter.return_value = {"result": "mock2"}

        filters = [mock_filter1, mock_filter2]

        chain = FilterFactory.create_filter_chain(filters)

        assert isinstance(chain, FilterChain)
        assert chain.filters == filters

        # Test that the chain works with the mocks
        result = chain.apply({"original": True})
        assert result == {"result": "mock2"}

    def test_create_filter_chain_with_any_filterlike(self):
        """Test that create_filter_chain works with any object implementing filter method."""

        class CustomFilter:
            def __init__(self, suffix):
                self.suffix = suffix

            def filter(self, message):
                message["custom"] = f"modified_{self.suffix}"
                return message

        filters = [CustomFilter("1"), CustomFilter("2")]

        chain = FilterFactory.create_filter_chain(filters)

        assert isinstance(chain, FilterChain)
        assert len(chain.filters) == 2

        # Test that the chain works with the custom filters
        result = chain.apply({"original": True})
        assert result == {"original": True, "custom": "modified_2"}


class TestFilterLikeProtocol:
    """Tests to verify FilterLike Protocol compatibility."""

    def test_messagefilter_is_filterlike(self):
        """Test that MessageFilter implementations satisfy the FilterLike Protocol."""

        class ConcreteFilter(MessageFilter):
            def filter(self, message):
                return message

        # This is more of a type checking test, but we can verify
        # that we can assign a MessageFilter to a variable expecting FilterLike
        def accepts_filterlike(f: FilterLike) -> bool:
            return True

        assert accepts_filterlike(ConcreteFilter())

    def test_mock_is_filterlike(self):
        """Test that mock objects satisfy the FilterLike Protocol."""
        mock_filter = MagicMock(spec=MessageFilter)

        # Verify the mock can be used in contexts expecting FilterLike
        chain = FilterChain([mock_filter])
        assert len(chain.filters) == 1

        # Set up mock behavior
        mock_filter.filter.return_value = {"mock": "response"}

        # Verify the mock works in the chain
        result = chain.apply({"test": "value"})
        assert result == {"mock": "response"}

    def test_any_object_with_filter_method_is_filterlike(self):
        """Test that any object with a filter method satisfies the FilterLike Protocol."""

        class CustomObject:
            def filter(self, message):
                message["custom"] = True
                return message

        # Verify the custom object can be used in contexts expecting FilterLike
        chain = FilterChain([CustomObject()])

        # Verify the custom object works in the chain
        result = chain.apply({"test": "value"})
        assert result == {"test": "value", "custom": True}
