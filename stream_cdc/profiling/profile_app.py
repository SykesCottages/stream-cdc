#!/usr/bin/env python3
import os
import sys
from typing import Any, Dict, List
import time
import argparse
from dotenv import load_dotenv

# Add the project to path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the profiler
from profiler import (
    FunctionProfiler,
    performance_tracker,
    patch_class_methods,
    time_profile,
    memory_profile,
    track_method_calls,
    logger
)

# Import app modules
from stream_cdc.utils.logger import Logger
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.streams.sqs import SQS
from stream_cdc.datasources.factory import DataSourceFactory
from stream_cdc.datasources.mysql import MySQLDataSource
from stream_cdc.state.factory import StateManagerFactory
from stream_cdc.state.dynamodb import Dynamodb
from stream_cdc.processing.coordinator import Coordinator, BatchSizeAndTimePolicy
from stream_cdc.processing.processors import DefaultEventProcessor
from stream_cdc.processing.worker import Worker
from stream_cdc.config.loader import AppConfig
from stream_cdc.utils.serializer import Serializer


def setup_profiling() -> None:
    """
    Set up profiling for key components of the application.
    """
    # List of key components to profile
    classes_to_profile = [
        # Stream class
        (SQS, ["send", "_prepare_sqs_entries", "_send_batch_to_sqs"]),

        # Data source
        (MySQLDataSource, [
            "connect", "listen", "_format_row_events",
            "_create_event_dict", "disconnect"
        ]),

        # State management
        (Dynamodb, ["store", "read", "_ensure_table_exists"]),

        # Coordinator
        (Coordinator, [
            "start", "process_next", "_process_event",
            "_flush_to_stream", "stop"
        ]),

        # Worker
        (Worker, ["run", "stop"]),

        # Utilities
        (Serializer, ["serialize"]),

        # Policy
        (BatchSizeAndTimePolicy, ["should_flush", "reset"])
    ]

    # Apply tracking to all listed classes
    for cls, methods in classes_to_profile:
        track_method_calls(cls, methods)

    logger.info("Profiling has been set up for key components")


def run_profiled_app(
    duration: float,
    profile_output: str
) -> None:
    """
    Run the application with profiling for a specific duration.

    Args:
        duration: Number of seconds to run the app
        profile_output: Directory to save profile output
    """
    load_dotenv()

    # Configure the profiler
    function_profiler = FunctionProfiler(output_dir=profile_output)

    # Set up profiling for key components
    setup_profiling()

    logger.info(f"Starting profiled run for {duration} seconds")

    # Create a modified main function similar to the original
    def app_main() -> None:
        logger_instance = Logger(log_level="INFO")
        logger = logger_instance.get_logger()

        app_config = AppConfig.load()

        if app_config.log_level != "INFO":
            logger_instance = Logger(log_level=app_config.log_level)
            logger = logger_instance.get_logger()

        stream_type = os.getenv("STREAM_TYPE", "sqs").lower()
        datasource_type = os.getenv("DS_TYPE", "mysql").lower()
        state_manager_type = os.getenv("STATE_MANAGER_TYPE", "dynamodb").lower()

        # Create the components needed by the coordinator
        stream_settings = {"source": os.getenv("DB_HOST")}
        stream = StreamFactory.create(stream_type, **stream_settings)
        datasource = DataSourceFactory.create(datasource_type)
        state_manager = StateManagerFactory.create(state_manager_type)
        event_processor = DefaultEventProcessor()
        flush_policy = BatchSizeAndTimePolicy(
            batch_size=app_config.batch_size,
            flush_interval=app_config.flush_interval
        )

        coordinator = Coordinator(
            datasource=datasource,
            state_manager=state_manager,
            stream=stream,
            event_processor=event_processor,
            flush_policy=flush_policy,
        )

        worker = Worker(coordinator)

        # Start the worker
        # Instead of running indefinitely, we'll run for the specified duration
        worker.coordinator.start()

        start_time = time.time()
        while time.time() - start_time < duration:
            worker.coordinator.process_next()

        # Cleanup
        worker.coordinator.stop()

        # Log performance metrics
        performance_tracker.log_summary()

        logger.info("Profiled run completed")

    # Profile the entire application run
    function_profiler.profile_function(app_main)


def main() -> None:
    """
    Main entry point with command line argument parsing.
    """
    parser = argparse.ArgumentParser(description="Profile the CDC application")
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Duration in seconds to run the application (default: 60)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="profiles",
        help="Directory to save profile output (default: 'profiles')"
    )

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    run_profiled_app(args.duration, args.output)


if __name__ == "__main__":
    main()

