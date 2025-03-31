from stream_cdc.streams.base import Stream
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.streams.sqs import SQS

# Register the SQS stream with the factory
StreamFactory.register_stream('sqs', SQS)

__all__ = [
    'Stream',
    'StreamFactory',
    'SQS'
]
