#!/bin/bash

# Build the Docker image for the analysis execution environment
echo "Building Docker image for analysis execution environment..."

# Navigate to the backend directory
cd "$(dirname "$0")"

# Build the Docker image
docker build -t code-execution-env .

echo "Docker image built successfully!"
echo "You can now start the backend API with: python main.py" 