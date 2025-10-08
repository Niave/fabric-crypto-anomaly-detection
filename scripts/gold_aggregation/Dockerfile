# Use slim Python base image with version 3.11
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# Install system dependencies needed by Snowpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy shared requirements file from project root into container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY gold_aggregation.py .

# Default command to run the script
CMD ["python", "gold_aggregation.py", "--step", "all"]

# To run the container with the .env file mounted from your local machine, use:
# docker run --rm -v "D:\BiznessVentures\Snowflake-Terraform\product-analytics-pipeline\.env:/app/.env" gold_aggregation
# docker run --rm -v "[Path-To-Your-.EnvFile]:/app/.env" gold_aggregation
