# Multi-stage Dockerfile for HouseTracker repo

# =============================================================================
# Stage 1: Base image with all dependencies
# =============================================================================
FROM python:3.12-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
ENV APP_PYTHON_HOME=/houseTracker/python
WORKDIR $APP_PYTHON_HOME

# Copy Pipfile for dependency installation
COPY Pipfile Pipfile.lock ./

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install pipenv && \
    pipenv install --system --deploy --ignore-pipfile

# Copy all source code
COPY shared/ ./shared/
COPY data_service/ ./data_service/
COPY crawler/ ./crawler/

# Set Python path
ENV PYTHONPATH=$APP_PYTHON_HOME

# =============================================================================
# Stage 2: Redfin Spider
# =============================================================================
FROM base AS redfin-spider

# Set working directory to redfin_spider
ENV APP_CRAWLER_HOME=$APP_PYTHON_HOME/crawler
ENV APP_REDFIN_SPIDER_HOME=$APP_CRAWLER_HOME/redfin_spider
WORKDIR $APP_CRAWLER_HOME

# Copy entrypoint script
COPY crawler/redfin_spider/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# =============================================================================
# Stage 3: Data Ingestion Service
# =============================================================================
FROM base AS data-ingestion-service

# Set working directory
WORKDIR $APP_PYTHON_HOME

# Copy entrypoint script
COPY data_service/entrypoints/data-ingestion-service-entrypoint.sh /usr/local/bin/data-ingestion-service-entrypoint.sh
RUN chmod +x /usr/local/bin/data-ingestion-service-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/data-ingestion-service-entrypoint.sh"]

# =============================================================================
# Stage 4: Property Scan Service
# =============================================================================
FROM base AS property-scan-service

# Set working directory
WORKDIR $APP_PYTHON_HOME

# Copy entrypoint script
COPY data_service/entrypoints/property-scan-service-entrypoint.sh /usr/local/bin/property-scan-service-entrypoint.sh
RUN chmod +x /usr/local/bin/property-scan-service-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/property-scan-service-entrypoint.sh"]
