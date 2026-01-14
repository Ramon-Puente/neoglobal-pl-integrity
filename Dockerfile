FROM python:3.11-slim

# Install system dependencies for build tools
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set dbt profiles directory
ENV DBT_PROFILES_DIR=/app/dbt_project

CMD ["bash"]