FROM python:3.11.1-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV FLASK_APP=execution_layer/execution_api.py
# Do not enable Flask debug/reloader in Cloud Run
# ENV FLASK_DEBUG=0
# NOTE: Do NOT set runtime secrets/config in the image (OpenAI key, bucket, api_layer_url, etc).
# All runtime env vars must be injected at deploy-time via Cloud Run env (single source of truth).
# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    procps \
    net-tools \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy application code - only the execution layer and dependencies
# Canonical logger is now under v1; copy it to /app/logger.py for existing imports.
COPY v1/logger.py ./logger.py
COPY execution_layer/ execution_layer/

# Create base output directory structure
RUN mkdir -p execution_layer/output_data && \
    chmod -R 777 execution_layer/output_data


# Expose default local port; Cloud Run injects PORT=8080 at runtime
EXPOSE 8080

# Run the execution API (use execution_api.py for full analysis capabilities)
CMD ["python", "execution_layer/execution_api.py"]
