#!/usr/bin/env python3
import os
import glob
import argparse
import pstats
import matplotlib.pyplot as plt
import re
from typing import Dict, List, Tuple, Optional


def parse_profiling_log(log_file: str) -> Dict[str, Dict[str, float]]:
    """
    Parse the profiling log and extract performance metrics.

    Args:
        log_file: Path to the profiling log file

    Returns:
        Dict with performance metrics by component
    """
    metrics = {}
    section_pattern = re.compile(
        r"Section ([A-Za-z0-9_\.]+): count=(\d+), avg=([\d\.]+)s, "
        r"min=([\d\.]+)s, max=([\d\.]+)s"
    )

    with open(log_file, "r") as f:
        for line in f:
            match = section_pattern.search(line)
            if match:
                section = match.group(1)
                count = int(match.group(2))
                avg = float(match.group(3))
                min_time = float(match.group(4))
                max_time = float(match.group(5))

                metrics[section] = {
                    "count": count,
                    "avg": avg,
                    "min": min_time,
                    "max": max_time,
                }

    return metrics


def analyze_profile_stats(
    profile_file: str,
) -> Tuple[List[Tuple[str, float]], List[Tuple[str, float]]]:
    """
    Analyze a profile statistics file.

    Args:
        profile_file: Path to the profile statistics file

    Returns:
        Tuple of (time_stats, call_stats)
    """
    # Load the stats
    p = pstats.Stats(profile_file)

    # Extract cumulative time stats
    p.sort_stats("cumtime")
    time_stats = []
    for func, (cc, nc, tt, ct, callers) in dict(p.stats).items():
        if "stream_cdc" in func[0]:  # Only include app functions
            time_stats.append((f"{func[2]} ({func[0]})", ct))

    # Sort by cumulative time and get top functions
    time_stats.sort(key=lambda x: x[1], reverse=True)
    time_stats = time_stats[:20]  # Get top 20

    # Extract call count stats
    p.sort_stats("calls")
    call_stats = []
    for func, (cc, nc, tt, ct, callers) in dict(p.stats).items():
        if "stream_cdc" in func[0]:  # Only include app functions
            call_stats.append((f"{func[2]} ({func[0]})", nc))

    # Sort by call count and get top functions
    call_stats.sort(key=lambda x: x[1], reverse=True)
    call_stats = call_stats[:20]  # Get top 20

    return time_stats, call_stats


def plot_section_metrics(
    metrics: Dict[str, Dict[str, float]], output_file: Optional[str] = None
) -> None:
    """
    Plot section metrics as bar charts.

    Args:
        metrics: Dictionary of metrics by section
        output_file: Optional path to save the plot
    """
    # Prepare data
    sections = list(metrics.keys())
    avg_times = [metrics[s]["avg"] for s in sections]
    max_times = [metrics[s]["max"] for s in sections]
    counts = [metrics[s]["count"] for s in sections]

    # Sort by average time
    sorted_indices = sorted(
        range(len(avg_times)), key=lambda i: avg_times[i], reverse=True
    )
    sections = [sections[i] for i in sorted_indices]
    avg_times = [avg_times[i] for i in sorted_indices]
    max_times = [max_times[i] for i in sorted_indices]
    counts = [counts[i] for i in sorted_indices]

    # Limit to top 15 sections
    if len(sections) > 15:
        sections = sections[:15]
        avg_times = avg_times[:15]
        max_times = max_times[:15]
        counts = counts[:15]

    # Create figure with 2 subplots (timing and counts)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 14))

    # Plot average and max times
    x = range(len(sections))
    ax1.barh(x, avg_times, label="Average Time (s)", color="b", alpha=0.6)
    ax1.barh(x, max_times, label="Max Time (s)", color="r", alpha=0.4)
    ax1.set_yticks(x)
    ax1.set_yticklabels([s.split(".")[-1] for s in sections])
    ax1.set_xlabel("Time (seconds)")
    ax1.set_title("Average and Maximum Execution Time by Section")
    ax1.legend()

    # Add component names as annotations
    for i, section in enumerate(sections):
        component = section.split(".")[0]
        ax1.annotate(
            component,
            xy=(0, i),
            xytext=(-5, 0),
            textcoords="offset points",
            va="center",
            ha="right",
            fontsize=8,
            color="gray",
        )

    # Plot call counts
    ax2.barh(x, counts, color="g", alpha=0.6)
    ax2.set_yticks(x)
    ax2.set_yticklabels([s.split(".")[-1] for s in sections])
    ax2.set_xlabel("Call Count")
    ax2.set_title("Number of Calls by Section")

    plt.tight_layout()

    if output_file:
        plt.savefig(output_file)
        print(f"Plot saved to {output_file}")
    else:
        plt.show()


def plot_profile_stats(
    time_stats: List[Tuple[str, float]],
    call_stats: List[Tuple[str, float]],
    output_file: Optional[str] = None,
) -> None:
    """
    Plot profile statistics as bar charts.

    Args:
        time_stats: List of (function_name, cumulative_time) tuples
        call_stats: List of (function_name, call_count) tuples
        output_file: Optional path to save the plot
    """
    # Create figure with 2 subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 14))

    # Plot cumulative time
    func_names = [t[0].split()[0] for t in time_stats]
    cum_times = [t[1] for t in time_stats]

    y_pos = range(len(func_names))
    ax1.barh(y_pos, cum_times, align="center", alpha=0.7)
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(func_names)
    ax1.set_xlabel("Cumulative Time (seconds)")
    ax1.set_title("Top Functions by Cumulative Time")

    # Plot call counts
    call_func_names = [t[0].split()[0] for t in call_stats]
    call_counts = [t[1] for t in call_stats]

    y_pos_calls = range(len(call_func_names))
    ax2.barh(y_pos_calls, call_counts, align="center", alpha=0.7)
    ax2.set_yticks(y_pos_calls)
    ax2.set_yticklabels(call_func_names)
    ax2.set_xlabel("Call Count")
    ax2.set_title("Top Functions by Call Count")

    plt.tight_layout()

    if output_file:
        plt.savefig(output_file)
        print(f"Plot saved to {output_file}")
    else:
        plt.show()


def generate_bottleneck_report(
    log_metrics: Dict[str, Dict[str, float]], time_stats: List[Tuple[str, float]]
) -> str:
    """
    Generate a report identifying bottlenecks.

    Args:
        log_metrics: Dictionary of metrics by section
        time_stats: List of (function_name, cumulative_time) tuples

    Returns:
        Report text
    """
    report = "## Performance Bottlenecks Report\n\n"

    # Identify slow methods
    report += "### Slowest Methods\n\n"
    sorted_sections = sorted(
        log_metrics.items(), key=lambda x: x[1]["avg"], reverse=True
    )

    for i, (section, metrics) in enumerate(sorted_sections[:5]):
        component, method = section.split(".", 1) if "." in section else (section, "")
        report += (
            f"{i + 1}. **{component}.{method}**\n"
            f"   - Average time: {metrics['avg']:.6f}s\n"
            f"   - Maximum time: {metrics['max']:.6f}s\n"
            f"   - Call count: {metrics['count']}\n\n"
        )

    # Identify functions with high cumulative time
    report += "### Highest Cumulative Time\n\n"
    for i, (func, time) in enumerate(time_stats[:5]):
        simple_name = func.split()[0]
        report += f"{i + 1}. **{simple_name}**\n   - Cumulative time: {time:.6f}s\n\n"

    # Identify frequently called functions
    report += "### Most Frequently Called Methods\n\n"
    sorted_by_calls = sorted(
        log_metrics.items(), key=lambda x: x[1]["count"], reverse=True
    )

    for i, (section, metrics) in enumerate(sorted_by_calls[:5]):
        component, method = section.split(".", 1) if "." in section else (section, "")
        report += (
            f"{i + 1}. **{component}.{method}**\n"
            f"   - Call count: {metrics['count']}\n"
            f"   - Average time: {metrics['avg']:.6f}s\n"
            f"   - Total estimated time: {metrics['avg'] * metrics['count']:.6f}s\n\n"
        )

    return report


def main() -> None:
    """
    Main entry point with command line argument parsing.
    """
    parser = argparse.ArgumentParser(description="Analyze profiling results")
    parser.add_argument(
        "--profiles",
        type=str,
        default="profiles",
        help="Directory containing profile output (default: 'profiles')",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="profiling.log",
        help="Profiling log file (default: 'profiling.log')",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="profile_analysis",
        help="Output directory for analysis results (default: 'profile_analysis')",
    )

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    # Parse the profiling log
    log_metrics = parse_profiling_log(args.log)

    # Plot section metrics
    plot_section_metrics(log_metrics, output_file=f"{args.output}/section_metrics.png")

    # Find the latest profile file
    profile_files = glob.glob(f"{args.profiles}/*.prof")
    if not profile_files:
        print(f"No profile files found in {args.profiles}")
        return

    latest_profile = max(profile_files, key=os.path.getctime)
    print(f"Analyzing profile: {latest_profile}")

    # Analyze the profile stats
    time_stats, call_stats = analyze_profile_stats(latest_profile)

    # Plot profile stats
    plot_profile_stats(
        time_stats, call_stats, output_file=f"{args.output}/profile_stats.png"
    )

    # Generate bottleneck report
    report = generate_bottleneck_report(log_metrics, time_stats)

    # Save the report
    with open(f"{args.output}/bottleneck_report.md", "w") as f:
        f.write(report)

    print(f"Analysis complete. Results saved to {args.output}")


if __name__ == "__main__":
    main()
