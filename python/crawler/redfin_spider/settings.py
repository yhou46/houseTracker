# Scrapy settings for redfin_spider project
# This project level settings and is applied to all spiders in the project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#    https://docs.scrapy.org/en/latest/topics/settings.html#built-in-settings-reference
#    https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#    https://docs.scrapy.org/en/latest/topics/spider-middleware.html



# Add shared folder to Python path
# This allows importing from python/shared/ from any Scrapy project
# shared_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'shared')
# sys.path.insert(0, shared_path)


# =============================================================================
# CRAWLER SETTINGS
# =============================================================================
# A list of modules where Scrapy will look for spiders.
SPIDER_MODULES = [
    "redfin_spider.spiders"
]
# Module where to create new spiders using the genspider command.
NEWSPIDER_MODULE = "redfin_spider.spiders"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "redfin_spider (+http://www.yourdomain.com)"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32
# CONCURRENT_REQUESTS = 2

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 2
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
# DOWNLOADER_MIDDLEWARES = {
#     "scrapy_playwright.middleware.PlaywrightMiddleware": 725,
# }

# # Playwright specific settings
# # Reference: https://github.com/scrapy-plugins/scrapy-playwright?tab=readme-ov-file#supported-settings
# DOWNLOAD_HANDLERS = {
#     "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
#     "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
# }
# # Playwright-specific settings
# PLAYWRIGHT_LAUNCH_OPTIONS = {
#     "headless": True,  # Run browser in background (no visible window)
#     "timeout": 20 * 1000,  # 20 seconds timeout
# }
# PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30000  # 30 seconds for page load

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# TODO: need to move to customzied settings
# ITEM_PIPELINES: Dict[str, int] = {
#     "redfin_spider.pipelines.JsonlPipeline": 300,
# }

# JSONL Pipeline settings
# TODO: need to move to customzied settings
# JSONL_OUTPUT_DIR = "redfin_output"
# JSONL_OUTPUT_FILE = None # Will use timestamp-based filename if None

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

# =============================================================================
# Default logging settings
# =============================================================================
## Create log folder if it doesn't exist
# logs_dir = os.path.join(os.path.dirname(__file__), '..', 'redfin_logs')
# os.makedirs(logs_dir, exist_ok=True)
# timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
# LOG_FILE = os.path.join(logs_dir, f'spider_{timestamp}.log')

LOG_LEVEL = 'INFO'
LOG_STDOUT = True
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FILE_APPEND = False
