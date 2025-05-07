import cProfile
import pstats
import io
import time
import tracemalloc
import logging
import os
import sys
from functools import wraps
from typing import Dict, Any, List, Callable, Optional, TypeVar, Union

# Type hints
F = TypeVar('F', bound=Callable[..., Any])

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("profiling.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("profiler")


def memory_profile(func: F) -> F:
    """
    Decorator to track memory usage of a function.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        tracemalloc.start()
        start_time = time.time()

        result = func(*args, **kwargs)

        elapsed_time = time.time() - start_time
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        logger.info(
            f"Function: {func.__name__} - "
            f"Memory usage: current={current / 1024:.2f}KB, peak={peak / 1024:.2f}KB, "
            f"Time: {elapsed_time:.5f} seconds"
        )
        return result

    return wrapper  # type: ignore


def time_profile(func: F) -> F:
    """
    Decorator to track execution time of a function.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time

        logger.info(f"Function: {func.__name__} - Time: {elapsed_time:.5f} seconds")
        return result

    return wrapper  # type: ignore


class FunctionProfiler:
    """
    Profiles functions using cProfile.
    """
    def __init__(self, output_dir: str = "profiles"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def profile_function(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """
        Profile a function and save results.
        """
        profiler = cProfile.Profile()
        profiler.enable()

        try:
            result = func(*args, **kwargs)
        finally:
            profiler.disable()

            # Save profiling stats
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(30)  # Print top 30 functions

            # Save to file
            filename = f"{self.output_dir}/{func.__name__}_{int(time.time())}.prof"
            ps.dump_stats(filename)

            logger.info(f"Profiling stats for {func.__name__}:\n{s.getvalue()}")
            logger.info(f"Full profiling stats saved to {filename}")

        return result


class PerformanceTracker:
    """
    Tracks various performance metrics during execution.
    """
    def __init__(self):
        self.metrics: Dict[str, List[Dict[str, Union[float, int]]]] = {}

    def start_tracking(self, name: str) -> int:
        """
        Start tracking a section of code.
        """
        if name not in self.metrics:
            self.metrics[name] = []

        start_id = len(self.metrics[name])
        self.metrics[name].append({
            'start_time': time.time(),
            'id': start_id
        })

        return start_id

    def stop_tracking(self, name: str, tracking_id: int, **extra_data: Any) -> None:
        """
        Stop tracking a section of code and record metrics.
        """
        if name not in self.metrics or tracking_id >= len(self.metrics[name]):
            logger.error(f"Cannot stop tracking {name}:{tracking_id} - not found")
            return

        end_time = time.time()
        metrics_entry = self.metrics[name][tracking_id]
        start_time = metrics_entry['start_time']

        # Update with duration and any extra data
        metrics_entry.update({
            'end_time': end_time,
            'duration': end_time - start_time,
            **extra_data
        })

    def get_metrics(self) -> Dict[str, List[Dict[str, Union[float, int]]]]:
        """
        Get all recorded metrics.
        """
        return self.metrics

    def get_average_duration(self, name: str) -> Optional[float]:
        """
        Get average duration for a tracked section.
        """
        if name not in self.metrics:
            return None

        durations = [
            m.get('duration', 0)
            for m in self.metrics[name]
            if 'duration' in m
        ]

        if not durations:
            return None

        return sum(durations) / len(durations)

    def log_summary(self) -> None:
        """
        Log a summary of all tracked metrics.
        """
        for name, measurements in self.metrics.items():
            completed = [m for m in measurements if 'duration' in m]
            if not completed:
                continue

            avg_duration = sum(m['duration'] for m in completed) / len(completed)
            max_duration = max(m['duration'] for m in completed)
            min_duration = min(m['duration'] for m in completed)

            logger.info(
                f"Section {name}: count={len(completed)}, "
                f"avg={avg_duration:.5f}s, min={min_duration:.5f}s, "
                f"max={max_duration:.5f}s"
            )


# Helper to patch methods for profiling
def patch_class_methods(
    cls: Any,
    method_names: List[str],
    decorator: Callable[[F], F]
) -> None:
    """
    Patch specified methods of a class with a decorator.
    """
    for method_name in method_names:
        if not hasattr(cls, method_name):
            logger.warning(f"Method {method_name} not found in class {cls.__name__}")
            continue

        original_method = getattr(cls, method_name)
        setattr(cls, method_name, decorator(original_method))


# Create a singleton performance tracker
performance_tracker = PerformanceTracker()
function_profiler = FunctionProfiler()


def track_method_calls(cls: Any, methods: List[str]) -> None:
    """
    Track method calls for a specific class.
    """
    for method_name in methods:
        if not hasattr(cls, method_name):
            continue

        original_method = getattr(cls, method_name)

        @wraps(original_method)
        def tracked_method(self: Any, *args: Any, **kwargs: Any) -> Any:
            tracking_id = performance_tracker.start_tracking(
                f"{cls.__name__}.{method_name}"
            )
            try:
                result = original_method(self, *args, **kwargs)
                return result
            finally:
                performance_tracker.stop_tracking(
                    f"{cls.__name__}.{method_name}",
                    tracking_id
                )

        setattr(cls, method_name, tracked_method)


def main() -> None:
    """
    Main function to demonstrate usage.
    """
    logger.info("Profiling example")

    @memory_profile
    def example_function() -> None:
        data = [i for i in range(1000000)]
        time.sleep(0.5)

    example_function()

    tracking_id = performance_tracker.start_tracking("main_section")
    time.sleep(1)
    performance_tracker.stop_tracking("main_section", tracking_id)

    performance_tracker.log_summary()


if __name__ == "__main__":
    main()

