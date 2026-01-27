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

# Copy shared modules (used by all spiders)
COPY shared/ ./shared/

# Set Python path to include shared modules
ENV PYTHONPATH=$APP_PYTHON_HOME

# =============================================================================
# Stage 2: Redfin Spider
# =============================================================================
FROM base AS redfin-spider

# Set working directory to redfin_spider
ENV APP_CRAWLER_HOME=$APP_PYTHON_HOME/crawler
ENV APP_REDFIN_SPIDER_HOME=$APP_CRAWLER_HOME/redfin_spider
WORKDIR $APP_CRAWLER_HOME

# Copy crawler files
COPY crawler/ $APP_CRAWLER_HOME/

# Copy entrypoint script
COPY crawler/redfin_spider/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
