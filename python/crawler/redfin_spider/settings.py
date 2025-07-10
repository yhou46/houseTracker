# Scrapy settings for redfin_spider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import sys
import os
import logging
from datetime import datetime

# Add shared folder to Python path
# This allows importing from python/shared/ from any Scrapy project
shared_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'shared')
sys.path.insert(0, shared_path)

BOT_NAME = "redfin_spider"

SPIDER_MODULES = ["redfin_spider.spiders"]
NEWSPIDER_MODULE = "redfin_spider.spiders"

ADDONS = {} # type: ignore

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

# Enable logging
LOG_ENABLED = True

# Set default log level
LOG_LEVEL = 'INFO'

# Create logs directory if it doesn't exist
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Generate timestamp for log files
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# Log file paths with different names for different levels
LOG_FILE = os.path.join(LOGS_DIR, f'spider_{TIMESTAMP}.log')
ERROR_LOG_FILE = os.path.join(LOGS_DIR, f'errors_{TIMESTAMP}.log')
DEBUG_LOG_FILE = os.path.join(LOGS_DIR, f'debug_{TIMESTAMP}.log')
PERFORMANCE_LOG_FILE = os.path.join(LOGS_DIR, f'performance_{TIMESTAMP}.log')

# Log format settings
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d_%H:%M:%S'

# Don't append to existing log files - start fresh each run
LOG_FILE_APPEND = False

# Configure logging handlers
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': LOG_FORMAT,
            'datefmt': LOG_DATEFORMAT,
        },
        'detailed': {
            'format': '%(asctime)s [%(name)s] %(levelname)s: %(message)s [%(filename)s:%(lineno)d]',
            'datefmt': LOG_DATEFORMAT,
        },
        'performance': {
            'format': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            'datefmt': LOG_DATEFORMAT,
        },
        'console_capture': {
            'format': '%(asctime)s [CONSOLE] %(levelname)s: %(message)s',
            'datefmt': LOG_DATEFORMAT,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'filename': LOG_FILE,
            'mode': 'w',
            'encoding': 'utf-8',
        },
        'error_file': {
            'class': 'logging.FileHandler',
            'level': 'ERROR',
            'formatter': 'detailed',
            'filename': ERROR_LOG_FILE,
            'mode': 'w',
            'encoding': 'utf-8',
        },
        'debug_file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': DEBUG_LOG_FILE,
            'mode': 'w',
            'encoding': 'utf-8',
        },
        'performance_file': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'performance',
            'filename': PERFORMANCE_LOG_FILE,
            'mode': 'w',
            'encoding': 'utf-8',
        },
        'console_capture': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'console_capture',
            'filename': LOG_FILE,
            'mode': 'a',  # Append to main log file
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console', 'file', 'error_file'],
            'level': 'INFO',
            'propagate': False,
        },
        'redfin_spider': {  # Spider-specific logger
            'handlers': ['console', 'file', 'error_file', 'debug_file'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'scrapy': {  # Scrapy framework logger
            'handlers': ['console', 'file', 'error_file'],
            'level': 'WARNING',
            'propagate': False,
        },
        'scrapy.core.engine': {  # Engine logs
            'handlers': ['console', 'file', 'performance_file'],
            'level': 'INFO',
            'propagate': False,
        },
        'scrapy.extensions': {  # Extension logs
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'scrapy.middleware': {  # Middleware logs
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'scrapy.stats': {  # Statistics logs
            'handlers': ['performance_file'],
            'level': 'INFO',
            'propagate': False,
        },
        'console_output': {  # Console output capture
            'handlers': ['console_capture'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

# =============================================================================
# CRAWLER SETTINGS
# =============================================================================

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "redfin_spider (+http://www.yourdomain.com)"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32
CONCURRENT_REQUESTS = 1

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "redfin_spider.middlewares.RedfinSpiderSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "redfin_spider.middlewares.RedfinSpiderDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "redfin_spider.pipelines.JsonlPipeline": 300,
}

# JSONL Pipeline settings
JSONL_OUTPUT_DIR = "output"
JSONL_OUTPUT_FILE = None  # Will use timestamp-based filename if None

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"
