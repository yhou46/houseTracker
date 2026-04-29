"""
Property Crawler Spider

This spider consumes property URLs from Redis Stream and crawls individual property pages.
Scraped raw property data is published to another Redis Stream for downstream processing.

Architecture:
    Redis Stream (property_url_stream) → RedisStreamConsumer → message_handler() → url_queue
    → start() → scrapy.Request → parse_property_page() → RedfinPropertyItem
    → RawDataPublisherPipeline → Redis Stream (raw_property_data_stream)
"""
import os
import asyncio
# from datetime import datetime, timezone
from typing import Any, Iterator

import scrapy
from scrapy.http import Response
from scrapy.exceptions import DropItem
import redis.asyncio as redis_async

from ..items import RedfinPropertyItem
from .utils import (
    setup_spider_logging,
)
# Import parse function from redfin_parser
from ..redfin_parser import parse_property_page
from ..pipelines import PropertyUrlMessageData
from ..aws_s3_pipeline import AwsS3Pipeline
from ..pipelines import JsonlPipeline

from shared.logger_factory import configure_logger
from shared.redis_stream_util import (
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    RedisStreamTrimConfig,
    RedisStreamMessage,
    get_message_data_type,
)
from shared.config_util import get_config_from_file


class PropertyCrawlerSpider(scrapy.Spider):
    """
    Spider for crawling individual property pages from Redis Stream.

    This spider:
    1. Consumes property URLs from Redis Stream via RedisStreamConsumer
    2. Generates Scrapy requests dynamically as URLs arrive
    3. Parses property pages to extract structured data
    4. Publishes raw property data to Redis Stream via pipeline
    5. Automatically shuts down when no messages for configured idle time

    Configuration:
        Config file: spiders/config/property_crawler_spider.config.json
    """

    name = "property_crawler_spider"
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

        # Crawler settings - similar to monolith spider
        "BOT_NAME": "property_crawler_spider",
        "CONCURRENT_REQUESTS": 3,
        "DOWNLOAD_DELAY": 2,

        # Pipelines - publish raw data to Redis Stream
        "ITEM_PIPELINES": {
            "redfin_spider.pipelines.RawDataPublisherPipeline": 100,
            "redfin_spider.aws_s3_pipeline.AwsS3Pipeline": 101,
            "redfin_spider.pipelines.JsonlPipeline": 200,
        },
    }

    @classmethod
    def from_crawler(cls, crawler, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
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

        try:
            # Load and apply config from JSON file
            config_file_prefix = "property_crawler_spider"
            config_file_path = os.path.join(
                os.path.dirname(__file__),
                "config",
            )
            config = get_config_from_file(
                config_file_prefix=config_file_prefix,
                config_file_path=config_file_path,
            )
            spider.logger.info(f"Loaded config from: {config_file_path}")

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

            # Set Redis stream config for publishing raw data
            raw_property_data_flow = config.get("raw_property_data_flow", {})
            spider.settings.set(
                "RAW_PROPERTY_DATA_STREAM_NAME",
                raw_property_data_flow.get("redis_stream_name", "raw_property_data_stream"),
                priority="spider"
            )

            # Set up AWS S3 settings for pipeline
            aws_s3_config = config.get("aws_s3")
            if not aws_s3_config:
                raise ValueError("Missing 'aws_s3' configuration in config file")
            aws_s3_bucket_name = aws_s3_config.get("bucket_name")
            aws_s3_region = aws_s3_config.get("region")
            spider.settings.set(
                AwsS3Pipeline.aws_s3_bucket_name_setting, aws_s3_bucket_name, priority="spider",
            )
            spider.settings.set(
                AwsS3Pipeline.aws_region_setting, aws_s3_region, priority="spider",
            )
            # Generate unique worker ID for S3 pipeline
            worker_id = AwsS3Pipeline.generate_worker_id(cls.name)
            spider.settings.set(AwsS3Pipeline.worker_id_setting, worker_id, priority="spider")
            spider.logger.info(f"Generated worker ID for S3 pipeline: {worker_id}")

            # Set json pipeline (debug only)
            output_directory = os.path.join(os.path.dirname(__file__), "..", f"{cls.name}_output")
            os.makedirs(output_directory, exist_ok=True)
            spider.settings.set(
                "JSONL_OUTPUT_DIR", output_directory, priority="spider",
            )
            output_file_prefix = "redfin_properties"
            output_filename = JsonlPipeline.generate_unique_file_name(output_file_prefix)
            spider.settings.set(
                "JSONL_OUTPUT_FILE", output_filename, priority="spider",
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
        Initialize spider and set up Redis consumer.
        """
        super().__init__(*args, **kwargs)

        # Config will be set by from_crawler
        self.config: dict[str, Any] = {}

        # Configure logger for other functions
        configure_logger(logger_override=self.logger)

        # Redis consumer and queue (will be initialized in start())
        self.redis_client: redis_async.Redis | None = None
        self.consumer: RedisStreamConsumer | None = None
        self.url_queue: asyncio.Queue[PropertyUrlMessageData] = asyncio.Queue()

        # Track metrics
        self.properties_crawled = 0
        self.properties_failed = 0
        self.urls_received = 0

    async def start(self):  # type: ignore[no-untyped-def]
        """
        Start Redis consumer and continuously generate requests from queue.

        This method:
        1. Initializes Redis client and consumer
        2. Starts consumer in background (runs continuously)
        3. Reads property URLs from queue
        4. Yields Scrapy requests for each URL
        5. Stops when consumer shuts down (idle timeout)
        """
        # Initialize Redis client
        redis_config = self.config.get("redis", {})
        self.redis_client = redis_async.Redis(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            password=redis_config.get("password"),
            decode_responses=True,
            socket_connect_timeout=5,
        )

        # Initialize Redis consumer
        property_url_flow = self.config.get("property_url_flow", {})
        redis_consumer_settings = self.config.get("redis_consumer_settings", {})

        consumer_config = RedisStreamConsumerConfig(
            stream_name=property_url_flow.get("redis_stream_name", "property_url_stream"),
            consumer_group=property_url_flow.get("redis_consumer_group", "property_url_consumer_group"),
            consumer_name_prefix=f"{self.name}",
            read_block_ms=redis_consumer_settings.get("read_block_ms", 5000),
            read_batch_size=redis_consumer_settings.get("read_batch_size", 10),
            read_delay_ms=redis_consumer_settings.get("read_delay_ms", None),
            claim_interval_seconds=redis_consumer_settings.get("claim_interval_seconds", 15),
            claim_idle_ms=redis_consumer_settings.get("claim_idle_ms", 60000),
            claim_count=redis_consumer_settings.get("claim_count", 10),
            processing_timeout_seconds=redis_consumer_settings.get("processing_timeout_seconds", 45),
            shutdown_when_idle_seconds=redis_consumer_settings.get("shutdown_when_idle_seconds", 60),
        )

        trimmer_config = RedisStreamTrimConfig(
            stream_name=property_url_flow.get("redis_stream_name", "property_url_stream"),
            trim_interval_seconds=redis_consumer_settings.get("trim_interval_seconds", 300),
            trim_max_len=redis_consumer_settings.get("trim_max_len", 10000),
            trim_approximate=redis_consumer_settings.get("trim_approximate", True),
        )

        # Trim trigger function - always trim for now
        async def always_trim() -> bool:
            return True

        self.consumer = RedisStreamConsumer(
            redis_client=self.redis_client,
            consumer_config=consumer_config,
            trimmer_config=trimmer_config,
            trim_trigger=always_trim,
            message_handler=self._handle_redis_message,
            debug=False,
        )

        # Start consumer in background
        await self.consumer.start()
        self.logger.info(f"Started Redis consumer: {consumer_config.consumer_group}")

        # Continuously consume from queue and generate requests
        self.logger.info("Starting to consume property URLs from queue...")

        while self.consumer.is_running():
            try:
                # Wait for URLs from queue (with timeout to check consumer status)
                message_data = await asyncio.wait_for(
                    self.url_queue.get(),
                    timeout=5.0
                )

                property_url = message_data.property_url
                from_page_url = message_data.from_page_url
                scraped_at_utc = message_data.scraped_at_utc
                property_id = message_data.property_id

                if not property_url:
                    self.logger.warning(f"Received message without property_url: {message_data}")
                    continue

                self.logger.info(f"Generating request for: {property_url}")

                # Yield Scrapy request for this property URL
                yield scrapy.Request(
                    url=property_url,
                    callback=self.parse_property_page,
                    errback=self.handle_error,
                    meta={
                        "from_page_url": from_page_url,
                        "scraped_at_utc": scraped_at_utc,
                        "property_id": property_id,
                    }
                )

            except asyncio.TimeoutError:
                # No messages in queue, continue waiting
                continue
            except Exception as e:
                self.logger.error(f"Error processing message from queue: {e}", exc_info=True)
                continue

        self.logger.info("Consumer stopped, no more URLs to process")

    async def _handle_redis_message(self, message: RedisStreamMessage) -> None:
        """
        Handle messages from Redis Stream.

        This callback is invoked by RedisStreamConsumer for each message.
        It extracts the property URL and puts it in the queue for the spider to process.

        Args:
            message: Redis stream message containing property URL data
        """
        try:
            self.urls_received += 1

            # Check type first
            message_data_type = get_message_data_type(message.data)
            if message_data_type != message_data_type.PROPERTY_URL:
                self.logger.error(
                    f"Unexpected message type: {message_data_type} "
                    f"in message ID: {message.redis_stream_message_id}"
                )
                return

            # Extract message data
            message_data = PropertyUrlMessageData.from_redis_fields(message.data)

            self.logger.debug(
                f"Received property URL from Redis: {message_data.property_url} "
                f"(message_id: {message.redis_stream_message_id})"
            )

            # Put message data into queue for spider to process
            await self.url_queue.put(message_data)

        except Exception as e:
            self.logger.error(
                f"Error handling Redis message {message.redis_stream_message_id}: {e}",
                exc_info=True
            )
            # Re-raise to let consumer handle retry logic
            raise

    def parse_property_page(self, response: Response): # type: ignore[no-untyped-def]
        """
        Parse individual property page to extract detailed information.
        """
        self.logger.info(f"Parsing property page: {response.url}")

        # Save HTML response for debugging
        # self._save_html_response(response, "property_page")

        try:
            # Use the parser module to extract data
            parsed_data = parse_property_page(
                url=response.url,
                html_content=response.text,
                spider_name=self.name
            )

            # Create item and populate it
            item = RedfinPropertyItem()
            for key, value in parsed_data.items():
                item[key] = value

            # Extract zip code using hybrid approach:
            # 1. First try to get from request meta (from URL-based search)
            zip_code = response.meta.get('zip_code')

            # 2. If not available, try to extract from address (for city-based searches or fallback)
            # Address format: "10508 135th Pl NE #37, Kirkland, WA 98033"
            # Zip code is the last entry when split by whitespace
            if not zip_code:
                address = item.get('address', '')
                if address:
                    address_parts = address.split()
                    if address_parts:
                        zip_code = address_parts[-1]
                        # Remove any trailing punctuation (e.g., comma, period)
                        zip_code = zip_code.rstrip(',.')
                        self.logger.debug(f"Extracted zip code from address: {zip_code}")

            # 3. Add zip code to item
            if zip_code:
                item['zipCode'] = zip_code
            else:
                # Last resort: use 'unknown' if zip code cannot be determined
                item['zipCode'] = 'unknown'
                self.logger.warning(f"No zip code found for property: {item.get('address', 'Unknown address')}")

            # 4. Pass through property_id from scan flow (None for new discovery)
            item['propertyId'] = response.meta.get('property_id')

            # Log the extracted data
            self.logger.info(f"Extracted property: {item.get('address', 'Unknown address')}, Redfin ID: {item.get('redfinId')}, Zip Code: {item.get('zipCode')}, Property ID if any: {item.get('propertyId')}")

            self.properties_crawled += 1

            yield item

        except Exception as error:
            self.logger.error(f"Spider error: failed to parse property page {response.url}: {error}")
            self.properties_failed += 1
            # raise DropItem(f"Failed to parse property page {response.url}: {error}")

    def handle_error(self, failure):  # type: ignore[no-untyped-def]
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
        self.properties_failed += 1

        # TODO: Implement dead letter queue for failed URLs
        # For now, just log and continue

    async def closed(self, reason: str) -> None:
        """
        Called when spider is closed.

        Stops Redis consumer and logs final metrics.

        Args:
            reason: Reason for spider closure
        """
        self.logger.info(f"Spider {self.name} closing: {reason}")

        # Stop consumer gracefully
        if self.consumer:
            self.logger.info("Stopping Redis consumer...")
            await self.consumer.stop()

        # Close Redis client
        if self.redis_client:
            await self.redis_client.aclose()

        # Log final metrics
        self.logger.info(
            f"Final metrics: "
            f"URLs received: {self.urls_received}, "
            f"Properties crawled: {self.properties_crawled}, "
            f"Properties failed: {self.properties_failed}"
        )

        self.logger.info(f"Spider {self.name} closed: {reason}")
