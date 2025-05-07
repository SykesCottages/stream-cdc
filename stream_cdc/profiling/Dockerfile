FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy profiling scripts first for better caching
COPY profiler.py /app/profiler.py
COPY profile_app.py /app/profile_app.py
COPY analyze_profile.py /app/analyze_profile.py

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY stream_cdc/ /app/stream_cdc/

# Create output directories
RUN mkdir -p /app/profiles /app/profile_analysis

# Make scripts executable
RUN chmod +x /app/profile_app.py /app/analyze_profile.py

# Set environment variables
ENV PYTHONUNBUFFERED=1

CMD ["bash"]
