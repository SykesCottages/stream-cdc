#!/bin/bash

# Script to run profiling and analysis in sequence
# Usage: ./run_profiling.sh [duration_in_seconds]

set -e  # Exit on error

# Default duration
DURATION=${1:-60}
PROFILE_DIR="profiles"
ANALYSIS_DIR="profile_analysis"

# Create directories if they don't exist
mkdir -p $PROFILE_DIR $ANALYSIS_DIR

echo "========================================"
echo "  Stream CDC Application Profiler"
echo "========================================"
echo "Duration: $DURATION seconds"
echo "Profile output: $PROFILE_DIR"
echo "Analysis output: $ANALYSIS_DIR"
echo "========================================"

# Run the profiler
echo "Starting profiling run..."
python profile_app.py --duration $DURATION --output $PROFILE_DIR

# Wait a moment for files to be flushed
sleep 1

# Run the analysis
echo "Analyzing profiling results..."
python analyze_profile.py --profiles $PROFILE_DIR --log profiling.log --output $ANALYSIS_DIR

echo "========================================"
echo "Profiling complete!"
echo ""
echo "Check the following files for results:"
echo "- $ANALYSIS_DIR/bottleneck_report.md (Summary report)"
echo "- $ANALYSIS_DIR/section_metrics.png (Performance charts)"
echo "- $ANALYSIS_DIR/profile_stats.png (Profile statistics)"
echo "========================================"
