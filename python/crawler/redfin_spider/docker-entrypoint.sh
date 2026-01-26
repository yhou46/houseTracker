#!/bin/bash
set -e

# Validate required environment variable
if [ -z "$SPIDER_NAME" ]; then
    echo "ERROR: SPIDER_NAME environment variable is not set!"
    echo "Please set SPIDER_NAME to one of: property_url_discovery_spider, property_crawler_spider"
    exit 1
fi

echo "========================================"
echo "Redfin Spider Container Starting"
echo "========================================"
echo "Spider Name: $SPIDER_NAME"
echo "Working Directory: $(pwd)"
echo "Python Version: $(python --version)"
echo "========================================"

# Run the spider
echo "Executing: scrapy crawl $SPIDER_NAME"
exec scrapy crawl "$SPIDER_NAME"
