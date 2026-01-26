"""
Property URL Discovery Spider

This spider crawls Redfin search result pages to discover property URLs.
Discovered URLs are published to Redis Stream for downstream processing
by Property Crawler Spider.

Architecture:
    Start URLs → parse_search_results() → PropertyUrlItem → RedisStreamPublisherPipeline → Redis Stream
"""
import os
from datetime import datetime, timezone
from typing import Any, Iterator, Set
from dataclasses import dataclass

import scrapy
from scrapy.http import Response

from ..items import PropertyUrlItem
from .utils import (
    load_json_config,
    setup_spider_logging,
    generate_start_urls_from_config,
    extract_property_urls_from_response,
    extract_current_page_number,
    find_next_pagination_link,
)
from shared.logger_factory import configure_logger


@dataclass
class PropertyUrlMessage:
    """
    Message structure for property URLs published to Redis Stream.
    This matches the format expected by the Property Crawler Spider.
    """
    property_url: str
    scraped_at_utc: str  # ISO formatted datetime string
    data_source: str     # e.g., "Redfin"
    from_page_url: str   # URL of the page where the property URL was found


class PropertyUrlDiscoverySpider(scrapy.Spider):
    """
    Spider for discovering property URLs from Redfin search results.

    This spider:
    1. Loads start URLs from JSON config file
    2. Parses search result pages to extract property URLs
    3. Handles pagination to discover all properties
    4. Publishes discovered URLs to Redis Stream via pipeline

    Configuration:
        Config file: spiders/config/property_url_discovery_spider.config.json
    """

    name = "property_url_discovery_spider"
    allowed_domains = ["redfin.com"]

    # Default custom settings (can be overridden by config file)
    custom_settings = {
        # Enable AsyncioSelectorReactor for asyncio support
        # This allows redis.asyncio to work with Scrapy's Twisted reactor
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",

        # Logging settings
        "LOG_LEVEL": "INFO",
        "LOG_STDOUT": True,
        "LOG_FORMAT": "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        "LOG_DATEFORMAT": "%Y-%m-%d %H:%M:%S",
        "LOG_FILE_APPEND": False,

        # Crawler settings - faster than monolith since only parsing search results
        "BOT_NAME": "property_url_discovery_spider",
        "CONCURRENT_REQUESTS": 5,
        "DOWNLOAD_DELAY": 1,

        # Pipelines - only Redis publisher, no file output
        "ITEM_PIPELINES": {
            "redfin_spider.pipelines.PropertyUrlPublisherPipeline": 100,
        },

        # No browser rendering needed for search results
        # "DOWNLOAD_HANDLERS": {
        #     "http": "scrapy.http.downloadermiddlewares.HttpDownloadHandler",
        #     "https": "scrapy.http.downloadermiddlewares.HttpDownloadHandler",
        # },
    }

    @classmethod
    def from_crawler(cls, crawler, *args: Any, **kwargs: Any): # type: ignore[no-untyped-def]
        """
        Create spider instance from crawler and configure settings.
        """
        spider = super().from_crawler(crawler, *args, **kwargs)

        # Set up logging
        log_file_path = setup_spider_logging(
            spider_name=cls.name,
            base_directory=os.path.dirname(__file__)
        )
        spider.settings.set("LOG_FILE", log_file_path, priority="spider")

        # Load and apply config from JSON file
        config_path = os.path.join(
            os.path.dirname(__file__),
            "config",
            "property_url_discovery_spider.config.json"
        )

        try:
            config = load_json_config(config_path)
            spider.logger.info(f"Loaded config from: {config_path}")

            # Store config for spider instance
            spider.config = config

            # Apply spider_settings from config (override defaults)
            spider_settings = config.get("spider_settings", {})
            for key, value in spider_settings.items():
                spider.settings.set(key, value, priority="spider")
                spider.logger.info(f"Applied config setting: {key} = {value}")

            # Set Redis config in settings for pipeline
            redis_config = config.get("redis", {})
            spider.settings.set("REDIS_HOST", redis_config.get("host", "localhost"), priority="spider")
            spider.settings.set("REDIS_PORT", redis_config.get("port", 6379), priority="spider")
            spider.settings.set("REDIS_PASSWORD", redis_config.get("password"), priority="spider")

            # Set Redis stream config
            property_url_flow = config.get("property_url_flow", {})
            spider.settings.set(
                "REDIS_STREAM_NAME",
                property_url_flow.get("redis_stream_name", "property_url_stream"),
                priority="spider"
            )

        except FileNotFoundError as e:
            spider.logger.error(f"Config file not found: {e}")
            raise
        except Exception as e:
            spider.logger.error(f"Error loading config: {e}", exc_info=True)
            raise

        return spider

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize spider and generate start URLs from config.
        """
        super().__init__(*args, **kwargs)

        # Config will be set by from_crawler
        self.config: dict[str, Any] = {}

        # Start URLs will be set in start_requests()
        self.start_urls: list[str] = []

        # Configure logger for other functions
        configure_logger(logger_override=self.logger)

        # Track metrics
        self.urls_discovered = 0
        self.pages_crawled = 0

        # Dedup
        self.url_set: Set[str] = set()

    async def start(self): # type: ignore[no-untyped-def]
        """
        Generate initial requests from config start URLs.
        """
        # Generate start URLs from config
        self.start_urls = generate_start_urls_from_config(self.config)

        self.logger.info(f"Generated {len(self.start_urls)} start URLs from config")

        for url in self.start_urls:
            self.logger.info(f"Start URL: {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results, # type: ignore[arg-type]
                errback=self.handle_error,
                meta={
                    "original_url": url,
                }
            )

    def parse_search_results(self, response: Response) -> Iterator[PropertyUrlItem | scrapy.Request]:
        """
        Parse search results page to extract property URLs and handle pagination.

        This method:
        1. Extracts property URLs from the current page
        2. Yields PropertyUrlItem for each discovered URL
        3. Follows pagination to discover more URLs

        Args:
            response: Scrapy Response from search results page

        Yields:
            PropertyUrlItem: For each discovered property URL
            scrapy.Request: For next pagination page (if exists)
        """
        self.pages_crawled += 1
        self.logger.info(f"Parsing search results from: {response.url}")

        # Extract property URLs from this page
        property_urls = extract_property_urls_from_response(response, self.logger)

        # Create timestamp for all URLs discovered from this page
        scraped_at_utc = datetime.now(timezone.utc).isoformat()

        # Yield PropertyUrlItem for each discovered URL
        for property_url in property_urls:

            if property_url in self.url_set:
                self.logger.warning(f"Found duplicate URL: {property_url} from input URL: {response.url}")
                continue

            self.urls_discovered += 1

            item = PropertyUrlItem()
            item["property_url"] = property_url
            item["from_page_url"] = response.url
            item["scraped_at_utc"] = scraped_at_utc
            item["data_source"] = "Redfin"

            self.url_set.add(property_url)

            yield item

        # Handle pagination
        current_page = extract_current_page_number(response.url)
        next_page_url = find_next_pagination_link(response, current_page, self.logger)

        if next_page_url:
            # Follow next page
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_search_results, # type: ignore[arg-type]
                errback=self.handle_error,
                meta={
                    "original_url": response.meta.get("original_url"),
                }
            )

    def handle_error(self, failure): # type: ignore[no-untyped-def]
        """
        Error handler for failed requests.

        Logs the error and continues (doesn't crash the spider).

        Args:
            failure: Twisted Failure object
        """
        request = failure.request
        self.logger.error(
            f"Request failed: {request.url}\n"
            f"Error: {failure.value}\n"
            f"Error type: {failure.type}"
        )

        # TODO: Implement dead letter queue for failed URLs
        # For now, just log and continue

    def closed(self, reason: str) -> None:
        """
        Called when spider is closed.

        Logs final metrics.

        Args:
            reason: Reason for spider closure
        """
        self.logger.info(f"Spider {self.name} closed: {reason}")
        self.logger.info(
            f"Final metrics: "
            f"Pages crawled: {self.pages_crawled}, "
            f"URLs discovered: {self.urls_discovered}"
        )
