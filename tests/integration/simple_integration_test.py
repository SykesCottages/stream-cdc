import json
from stream_cdc.filters.base import FilterChain, MessageFilter, Message


class MockStorage:
    def __init__(self):
        self.stored_data = {}

    def store(self, data: str, key_prefix: str = "test") -> str:
        key = f"{key_prefix}/item_{len(self.stored_data) + 1}"
        self.stored_data[key] = data
        return f"mock-storage://{key}"


class RedactFilter(MessageFilter):
    def filter(self, message: Message) -> Message:
        filtered_message = message.copy()
        for key, value in message.items():
            if value == "small_message":
                filtered_message[key] = "haha u gone!"
                print(key)
        return filtered_message


class SizeFilter(MessageFilter):
    def __init__(self, storage: MockStorage, size_threshold: int = 50000):
        self.storage = storage
        self.size_threshold = size_threshold

    def filter(self, message: Message) -> Message:
        message_json = json.dumps(message)
        message_size = len(message_json.encode("utf-8"))

        if message_size <= self.size_threshold:
            return message

        filtered_message = message.copy()
        for key, value in message.items():
            if isinstance(value, str) and len(value) > 1000:
                storage_uri = self.storage.store(json.dumps(value))
                filtered_message[key] = storage_uri

                if (
                    len(json.dumps(filtered_message).encode("utf-8"))
                    <= self.size_threshold
                ):
                    break

        return filtered_message


def test_size_filtering():
    test_messages = [
        {
            "id": "small_message",
            "content": "This is a small message that won't be filtered",
            "metadata": {"type": "test"},
        },
        {
            "id": "large_message",
            "content": "X" * 100000,
            "metadata": {"type": "test"},
        },
        {
            "id": "mixed_message",
            "content": "Normal content",
            "large_field1": "Y" * 30000,
            "large_field2": "Z" * 30000,
            "metadata": {"type": "test"},
        },
    ]

    storage = MockStorage()
    size_filter = SizeFilter(storage, size_threshold=50000)
    redact_filter = RedactFilter()
    filter_chain = FilterChain([size_filter, redact_filter])

    for i, message in enumerate(test_messages):
        print(f"\n--- Testing Message {i + 1}: {message['id']} ---")

        original_size = len(json.dumps(message).encode("utf-8"))
        print(f"Original message size: {original_size} bytes")

        filtered = filter_chain.apply(message)
        filtered_size = len(json.dumps(filtered).encode("utf-8"))
        print(f"Filtered message size: {filtered_size} bytes")

        changes = []
        for key in message:
            if message[key] != filtered[key]:
                changes.append(key)

        if changes:
            print(f"Fields replaced: {', '.join(changes)}")
            for key in changes:
                print(
                    f"  - Original: {str(message[key])[:30]}... "
                    f"({len(str(message[key]))} chars)"
                )
                print(f"  - Filtered: {filtered[key]}")
        else:
            print(f"  - Original: {str(message)}... ")
            print(f"  - Filtered: {filtered}")

        print(f"Storage has {len(storage.stored_data)} items")


if __name__ == "__main__":
    test_size_filtering()
