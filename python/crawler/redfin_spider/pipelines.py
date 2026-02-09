# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Any, Self, cast
from itemadapter import ItemAdapter
import uuid

# Scrapy asyncio integration utilities
import scrapy
from scrapy.utils.defer import deferred_from_coro
from scrapy.crawler import Crawler
from scrapy.exceptions import DropItem


# Redis imports
import redis.asyncio as redis_async

# houseTracker imports
from shared.aws_s3_util import upload_json_objects
from shared.redis_stream_util import (
    RedisStreamProducer,
    RedisStreamProducerConfig,
    RedisFields,
    RedisStreamMessageData,
    RedisStreamMessageDataType,
    get_message_data_type,
    MessageParsingError
)
from shared.utils import generate_unique_time_based_str

class JsonlPipeline:
    """
    Pipeline to save scraped items to a JSONL file.

    Each item is written as a single JSON line to the output file.
    """

    def __init__(self) -> None:
        self.output_file: str | None = None
        self.output_dir: str | None = None

    @classmethod
    def from_crawler(cls, crawler): # type: ignore[no-untyped-def]
        """
        Create pipeline instance from crawler settings.
        """
        pipeline = cls()

        # Get output directory from settings, default to 'output'
        pipeline.output_dir = crawler.settings.get('JSONL_OUTPUT_DIR', 'output')

        # Get output filename from settings, default to timestamp-based name
        output_filename = crawler.settings.get('JSONL_OUTPUT_FILE')
        if not output_filename:
            unique_str = generate_unique_time_based_str("redfin_properties")
            output_filename = f"{unique_str}.jsonl"

        pipeline.output_file = output_filename

        return pipeline

    def open_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider opens. Create output directory and file.
        """
        if not self.output_dir or not self.output_file:
            raise ValueError("Output directory or file not set in JsonlPipeline")

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Full path to output file
        self.filepath = os.path.join(self.output_dir, self.output_file)

        # Open file for writing
        self.file = open(self.filepath, 'w', encoding='utf-8')

        spider.logger.info(f"JSONL Pipeline: Output file opened at {self.filepath}")

    def close_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider closes. Close the output file.
        """
        if hasattr(self, 'file') and self.file:
            self.file.close()
            spider.logger.info(f"JSONL Pipeline: Output file closed. Total records written: {getattr(self, 'record_count', 0)}")

    def process_item(self, item, spider): # type: ignore[no-untyped-def]
        """
        Process each scraped item and write it to the JSONL file.
        """
        try:
            # Convert item to dictionary
            item_dict = ItemAdapter(item).asdict()

            # Write item as JSON line
            json_line = json.dumps(item_dict, ensure_ascii=False)
            self.file.write(json_line + '\n')
            self.file.flush()  # Ensure data is written immediately

            # Track record count
            if not hasattr(self, 'record_count'):
                self.record_count = 0
            self.record_count += 1

            # Log progress every 10 records
            if self.record_count % 10 == 0:
                spider.logger.info(f"JSONL Pipeline: Written {self.record_count} records to {self.filepath}")

            return item

        except Exception as e:
            spider.logger.error(f"JSONL Pipeline: Error writing item to file: {e}")
            # Don't crash the spider, just log the error and continue
            return item

    @staticmethod
    def generate_unique_file_name(prefix: str) -> str:
        """
        Generate a unique worker ID using prefix, timestamp, and short UUID.

        Args:
            prefix: Prefix for the worker ID (typically spider name)

        Returns:
            Unique worker ID in format: {prefix}_{timestamp}_{short_uuid}
            Example: property_crawler_spider_20260114_063052_a7f3b9c2
        """
        unique_str = generate_unique_time_based_str(prefix)
        return f"{unique_str}.jsonl"

class RedisStreamPublisherPipeline(ABC):
    """
    Abstract base pipeline for publishing items to Redis Stream.

    This pipeline provides common functionality for batching items and publishing
    them to Redis Stream using RedisStreamProducer. Subclasses must implement
    the abstract methods to customize item processing and validation.

    Note: This uses Twisted's Deferred to integrate async Redis operations with Scrapy.
    We bridge asyncio and Twisted using deferred_from_coro() with AsyncioSelectorReactor.

    Subclasses must implement:
    - _item_to_redis_fields(): Convert item dict to Redis fields
    - _validate_item(): Validate item has required fields
    - _handle_publish_error(): Handle errors from async publish operation

    Subclasses should set class attributes:
    - stream_name_setting: Setting key for stream name (default: "REDIS_STREAM_NAME")
    - batch_size_setting: Setting key for batch size (default: "REDIS_BATCH_SIZE")
    - default_stream_name: Default stream name (default: "redis_stream")
    - default_batch_size: Default batch size (default: 100)
    """

    # Class attributes that subclasses can override
    redis_host_setting: str = "REDIS_HOST"
    redis_port_setting: str = "REDIS_PORT"
    redis_password_setting: str = "REDIS_PASSWORD"
    stream_name_setting: str = "REDIS_STREAM_NAME"
    batch_size_setting: str = "REDIS_BATCH_SIZE"
    default_stream_name: str = "redis_stream"
    default_batch_size: int = 100

    def __init__(self) -> None:
        self.redis_host: str = "localhost"
        self.redis_port: int = 6379
        self.redis_password: str | None = None
        self.stream_name: str = self.default_stream_name
        self.batch_size: int = self.default_batch_size

        # Batching state
        self.batch: list[dict[str, Any]] = []
        self.total_published: int = 0
        self.total_failed: int = 0

        # Redis client and producer (initialized in open_spider)
        self.redis_client: redis_async.Redis | None = None
        self.redis_producer: RedisStreamProducer | None = None

    # Scrapy lifecycle methods
    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        """
        Create pipeline instance from crawler settings.

        Uses class attributes to determine which settings to read.
        """
        pipeline = cls()

        # Get Redis connection settings
        pipeline.redis_host = crawler.settings.get(cls.redis_host_setting, 'localhost')
        pipeline.redis_port = crawler.settings.getint(cls.redis_port_setting, 6379)
        pipeline.redis_password = crawler.settings.get(cls.redis_password_setting)

        # Get stream name and batch size using class attributes
        pipeline.stream_name = crawler.settings.get(
            cls.stream_name_setting,
            cls.default_stream_name
        )
        pipeline.batch_size = crawler.settings.getint(
            cls.batch_size_setting,
            cls.default_batch_size
        )

        return pipeline

    def open_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider opens. Initialize Redis connection and producer.
        """
        pipeline_name = self.__class__.__name__
        spider.logger.info(
            f"{pipeline_name}: Initializing with "
            f"host={self.redis_host}, port={self.redis_port}, "
            f"stream={self.stream_name}, batch_size={self.batch_size}"
        )

        # Create async Redis client
        self.redis_client = redis_async.Redis(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password,
            decode_responses=True,
            socket_connect_timeout=5,
        )

        # Create producer config
        producer_config = RedisStreamProducerConfig(
            stream_name=self.stream_name,
            max_batch_size=self.batch_size
        )

        # Create Redis stream producer
        self.redis_producer = RedisStreamProducer(
            redis_client=self.redis_client,
            config=producer_config
        )

        spider.logger.info(f"{pipeline_name}: Initialized successfully")

    def close_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider closes. Flush any remaining items in batch, then close Redis connection.

        Ensures operations happen in order:
        1. Flush remaining batch (if any)
        2. Close Redis connection
        3. Log final stats
        """
        pipeline_name = self.__class__.__name__

        # Helper to log final stats
        def log_final_stats() -> None:
            spider.logger.info(
                f"{pipeline_name}: Closed. "
                f"Total published: {self.total_published}, "
                f"Total failed: {self.total_failed}"
            )
            return

        def handle_connection_close(failure: Any) -> None:
            if self.redis_client:
                spider.logger.info(f"{pipeline_name}: Redis connection closed")
            log_final_stats()
            return

        # Publish any remaining items in batch
        if self.batch:
            spider.logger.info(
                f"{pipeline_name}: Flushing {len(self.batch)} remaining items"
            )
            batch_to_publish = self.batch.copy()
            self.batch = []

            # Create deferred for batch flush
            deferred = deferred_from_coro(self._publish_batch_async(batch_to_publish, spider))

            # Chain: flush → close connection → log stats
            def handle_flush_success(result): # type: ignore[no-untyped-def]
                spider.logger.info(f"{pipeline_name}: Flush completed")
                # Now close Redis connection
                if self.redis_client:
                    spider.logger.info(f"{pipeline_name}: Closing Redis connection")
                    return deferred_from_coro(self.redis_client.aclose())
                return result

            def handle_flush_error(failure): # type: ignore[no-untyped-def]
                spider.logger.error(f"{pipeline_name}: Flush failed: {failure.getErrorMessage()}")
                # Still close connection even if flush failed
                if self.redis_client:
                    spider.logger.info(f"{pipeline_name}: Closing Redis connection (after flush error)")
                    return deferred_from_coro(self.redis_client.aclose())
                return failure

            deferred.addCallback(handle_flush_success)
            deferred.addErrback(handle_flush_error)
            deferred.addBoth(handle_connection_close)  # addBoth runs on success OR failure

        else:
            # No batch to flush, just close connection
            if self.redis_client:
                spider.logger.info(f"{pipeline_name}: Closing Redis connection")
                close_deferred = deferred_from_coro(self.redis_client.aclose())
                close_deferred.addBoth(handle_connection_close)
            else:
                # No batch, no connection - just log final stats immediately
                log_final_stats()

    def process_item(self, item, spider): # type: ignore[no-untyped-def]
        """
        Process each item and add to batch.

        When batch reaches batch_size, publish to Redis Stream.

        Returns:
            Deferred that fires when item is processed (batched or published)
        """
        try:
            # Convert item to dict
            item_dict = ItemAdapter(item).asdict()

            # Validate item using subclass implementation
            if not self._validate_item(item_dict, spider):
                self.total_failed += 1
                return item

            # Add to batch
            self.batch.append(item_dict)

            # Publish batch if batch size reached
            if len(self.batch) >= self.batch_size:
                batch_to_publish = self.batch.copy()
                self.batch = []

                # Return deferred for async publishing
                # deferred_from_coro bridges asyncio coroutine to Twisted Deferred
                # This works with AsyncioSelectorReactor
                deferred = deferred_from_coro(self._publish_batch_async(batch_to_publish, spider))

                # Attach callbacks to return item after publish completes
                def return_item_on_success(result): # type: ignore[no-untyped-def]
                    return item

                def return_item_on_error(failure): # type: ignore[no-untyped-def]
                    # Call subclass error handler
                    self._handle_publish_error(failure, spider, item)
                    return item

                deferred.addCallback(return_item_on_success)
                deferred.addErrback(return_item_on_error)

                return deferred

            # Item batched, return immediately
            return item

        except Exception as e:
            pipeline_name = self.__class__.__name__
            spider.logger.error(f"{pipeline_name}: Error processing item: {e}", exc_info=True)
            self.total_failed += 1
            return item

    async def _publish_batch_async(self, batch: list[dict[str, Any]], spider) -> None: # type: ignore[no-untyped-def]
        """
        Async method to publish a batch of items to Redis Stream.

        This is an asyncio coroutine that uses RedisStreamProducer.publish_batch().
        It's bridged to Twisted's Deferred system via deferred_from_coro() in process_item().

        Args:
            batch: List of item dictionaries to publish
            spider: Spider instance for logging
        """
        try:
            pipeline_name = self.__class__.__name__
            spider.logger.info(
                f"{pipeline_name}: Publishing batch of {len(batch)} items to stream '{self.stream_name}'"
            )

            # Convert items to Redis fields format using subclass implementation
            redis_messages: List[RedisFields] = []
            for item in batch:
                redis_message_data = self._item_to_redis_fields(item)
                redis_messages.append(redis_message_data.to_redis_fields())

            # Use RedisStreamProducer to publish batch
            if self.redis_producer is None:
                raise ValueError(f"Redis producer not initialized in {pipeline_name}")
            message_ids = await self.redis_producer.publish_batch(redis_messages)

            self.total_published += len(batch)

            spider.logger.info(
                f"{pipeline_name}: Successfully published {len(batch)} items. "
                f"Total published: {self.total_published}"
            )

        except Exception as e:
            pipeline_name = self.__class__.__name__
            spider.logger.error(
                f"{pipeline_name}: Failed to publish batch: {e}",
                exc_info=True
            )
            self.total_failed += len(batch)

            # TODO: Implement dead letter queue for failed publishes
            # For now, just log and continue
            raise  # Re-raise to propagate to Twisted errback

    # Abstract methods that subclasses must implement
    @abstractmethod
    def _item_to_redis_fields(self, item_dict: dict[str, Any]) -> RedisStreamMessageData:
        """
        Convert item dictionary to Redis fields format.

        Subclasses must implement this to define how items are serialized
        for Redis Stream publication.

        Args:
            item_dict: Item dictionary from ItemAdapter

        Returns:
            RedisFields dictionary for Redis Stream
        """
        pass

    @abstractmethod
    def _validate_item(self, item_dict: dict[str, Any], spider: scrapy.Spider) -> bool:
        """
        Validate item has required fields.

        Subclasses must implement this to define validation logic.

        Args:
            item_dict: Item dictionary from ItemAdapter
            spider: Spider instance for logging

        Returns:
            True if item is valid, False otherwise
        """
        pass

    @abstractmethod
    def _handle_publish_error(self, failure: Any, spider: scrapy.Spider, item: Any) -> Any:
        """
        Handle errors from async publish operation.

        Subclasses must implement this to define error handling logic.

        Args:
            failure: Twisted Failure object
            spider: Spider instance for logging
            item: Original item being processed

        Returns:
            The original item (to continue pipeline)
        """
        pass

class PropertyUrlMessageData(RedisStreamMessageData):
    """
    Redis Stream message data for PropertyUrlItem.
    """

    def __init__(
            self,
            property_url: str,
            scraped_at_utc: str,
            data_source: str,
            from_page_url: str,
            ) -> None:
        super().__init__(RedisStreamMessageDataType.PROPERTY_URL)
        self.property_url = property_url
        self.scraped_at_utc = scraped_at_utc
        self.data_source = data_source
        self.from_page_url = from_page_url

    @classmethod
    def from_redis_fields(cls, fields: RedisFields) -> Self:
        assert get_message_data_type(fields) == RedisStreamMessageDataType.PROPERTY_URL

        if not cls._validate_redis_fields(fields):
            raise MessageParsingError(
                f"Invalid Redis fields for {cls.__name__}",
                fields,
                get_message_data_type(fields),
            )

        return cls(
            property_url=cast(str, fields['property_url']),
            scraped_at_utc=cast(str, fields['scraped_at_utc']),
            data_source=cast(str, fields['data_source']),
            from_page_url=cast(str, fields['from_page_url']),
        )

    def to_redis_fields(self) -> RedisFields:
        """
        Convert PropertyUrlMessageData to Redis fields dictionary.
        """
        redis_fields = super().to_redis_fields()
        redis_fields.update({
            'property_url': self.property_url,
            'scraped_at_utc': self.scraped_at_utc,
            'data_source': self.data_source,
            'from_page_url': self.from_page_url,
        })
        return redis_fields

    @classmethod
    def _validate_redis_fields(cls, fields: RedisFields) -> bool:
        """
        Validate required fields for PropertyUrlMessageData.
        """
        required_fields = ['property_url', 'scraped_at_utc', 'data_source', 'from_page_url']
        for field in required_fields:
            if field not in fields or not fields[field] or not isinstance(fields[field], str):
                return False
        return True

class RawPropertyMessageData(RedisStreamMessageData):
    """
    Redis Stream message data for raw property data (RedfinPropertyItem).

    Contains the entire property data as JSON plus metadata fields for filtering/routing.
    """

    def __init__(
            self,
            data: str,
            url: str,
            redfin_id: str,
            zip_code: str,
            scraped_at: str,
            spider_name: str,
    ) -> None:
        super().__init__(RedisStreamMessageDataType.PROPERTY_RAW_DATA)
        self.data = data  # JSON serialized property data
        self.url = url
        self.redfin_id = redfin_id
        self.zip_code = zip_code
        self.scraped_at = scraped_at
        self.spider_name = spider_name

    @classmethod
    def from_redis_fields(cls, fields: RedisFields) -> Self:
        message_type = get_message_data_type(fields)

        if message_type != RedisStreamMessageDataType.PROPERTY_RAW_DATA:
            raise MessageParsingError(
                f"Expected PROPERTY_RAW_DATA type, got {message_type.value}",
                fields=fields,
                message_type=message_type
            )

        if not cls._validate_redis_fields(fields):
            raise MessageParsingError(
                f"Invalid Redis fields for {cls.__name__}",
                fields=fields,
                message_type=message_type
            )

        return cls(
            data=cast(str, fields['data']),
            url=cast(str, fields['url']),
            redfin_id=cast(str, fields['redfinId']),
            zip_code=cast(str, fields['zipCode']),
            scraped_at=cast(str, fields['scrapedAt']),
            spider_name=cast(str, fields['spiderName']),
        )

    def to_redis_fields(self) -> RedisFields:
        """
        Convert RawPropertyMessageData to Redis fields dictionary.
        """
        redis_fields = super().to_redis_fields()
        redis_fields.update({
            'data': self.data,
            'url': self.url,
            'redfinId': self.redfin_id,
            'zipCode': self.zip_code,
            'scrapedAt': self.scraped_at,
            'spiderName': self.spider_name,
        })
        return redis_fields

    @classmethod
    def _validate_redis_fields(cls, fields: RedisFields) -> bool:
        """
        Validate required fields for RawPropertyMessageData.
        """
        required_fields = ['data', 'url', 'redfinId', 'zipCode', 'scrapedAt', 'spiderName']
        for field in required_fields:
            if field not in fields or not isinstance(fields[field], str):
                return False
        return True

class PropertyUrlPublisherPipeline(RedisStreamPublisherPipeline):
    """
    Pipeline to publish property URLs to Redis Stream.

    This pipeline is used by PropertyUrlDiscoverySpider to publish discovered
    property URLs to Redis Stream for consumption by PropertyCrawlerSpider.

    Inherits all common Redis publishing logic from RedisStreamPublisherPipeline.
    """

    # Configure stream name and batch size settings
    # stream_name_setting = "REDIS_STREAM_NAME"
    # batch_size_setting = "REDIS_BATCH_SIZE"
    # default_stream_name = "property_url_stream"
    # default_batch_size = 100

    def _item_to_redis_fields(self, item_dict: dict[str, Any]) -> PropertyUrlMessageData:
        """
        Convert PropertyUrlItem to Redis fields format.

        Args:
            item_dict: PropertyUrlItem dictionary from ItemAdapter

        Returns:
            RedisFields dictionary with property URL fields
        """
            # redis_fields: RedisFields = {
            #     'property_url': item_dict['property_url'],
            #     'scraped_at_utc': item_dict['scraped_at_utc'],
            #     'data_source': item_dict['data_source'],
            #     'from_page_url': item_dict['from_page_url'],
            # }
        return PropertyUrlMessageData(
            property_url=cast(str, item_dict['property_url']),
            scraped_at_utc=cast(str, item_dict['scraped_at_utc']),
            data_source=cast(str, item_dict['data_source']),
            from_page_url=cast(str, item_dict['from_page_url']),
        )

    def _validate_item(self, item_dict: dict[str, Any], spider: scrapy.Spider) -> bool:
        """
        Validate PropertyUrlItem has all required fields.

        Args:
            item_dict: PropertyUrlItem dictionary from ItemAdapter
            spider: Spider instance for logging

        Returns:
            True if item is valid, False otherwise
        """
        required_fields = ['property_url', 'scraped_at_utc', 'data_source', 'from_page_url']
        for field in required_fields:
            if field not in item_dict or not item_dict[field]:
                spider.logger.error(
                    f"{self.__class__.__name__}: Missing required field '{field}' in item"
                )
                return False
        return True

    def _handle_publish_error(self, failure: Any, spider: scrapy.Spider, item: Any) -> Any:
        """
        Handle errors from async publish operation.

        Logs the error and returns the item to continue pipeline.

        Args:
            failure: Twisted Failure object
            spider: Spider instance for logging
            item: Original item being processed

        Returns:
            The original item (to continue pipeline)
        """
        spider.logger.error(
            f"{self.__class__.__name__}: Publish failed: {failure.getErrorMessage()}",
            exc_info=True
        )
        return item



class RawDataPublisherPipeline(RedisStreamPublisherPipeline):
    """
    Pipeline to publish raw property data to Redis Stream.

    This pipeline is used by PropertyCrawlerSpider to publish scraped property data
    to Redis Stream for consumption by downstream services (DB service, etc.).

    Inherits all common Redis publishing logic from RedisStreamPublisherPipeline.
    """

    # Configure stream name and batch size settings
    stream_name_setting = "RAW_PROPERTY_DATA_STREAM_NAME"
    batch_size_setting = "RAW_DATA_BATCH_SIZE"
    default_stream_name = "raw_property_data_stream"
    default_batch_size = 10  # Smaller batch size since property data is larger

    def _item_to_redis_fields(self, item_dict: dict[str, Any]) -> RawPropertyMessageData:
        """
        Convert RedfinPropertyItem to Redis fields format.

        Serializes the entire property item as JSON in the 'data' field,
        plus adds metadata fields for quick filtering/routing.

        Args:
            item_dict: RedfinPropertyItem dictionary from ItemAdapter

        Returns:
            RawPropertyMessageData with serialized property data
        """
        # Serialize entire item as JSON string
        data_json = json.dumps(item_dict, ensure_ascii=False)

        return RawPropertyMessageData(
            data=data_json,
            url=item_dict.get('url', ''),
            redfin_id=item_dict.get('redfinId', ''),
            zip_code=item_dict.get('zipCode', ''),
            scraped_at=item_dict.get('scrapedAt', ''),
            spider_name=item_dict.get('spiderName', ''),
        )

    def _validate_item(self, item_dict: dict[str, Any], spider: scrapy.Spider) -> bool:
        """
        Validate RedfinPropertyItem has required fields.

        Uses lenient validation - logs warnings for missing fields but returns True
        to allow partial data to be published (downstream services can filter).

        Args:
            item_dict: RedfinPropertyItem dictionary from ItemAdapter
            spider: Spider instance for logging

        Returns:
            True (always allows items through, just logs warnings)
        """
        required_fields = ['url', 'scrapedAt', 'spiderName']
        for field in required_fields:
            if field not in item_dict or not item_dict[field]:
                spider.logger.warning(
                    f"{self.__class__.__name__}: Missing required field '{field}' in item"
                )
                # Don't fail - just log warning and continue

        # Always return True (lenient validation)
        return True

    def _handle_publish_error(self, failure: Any, spider: scrapy.Spider, item: Any) -> Any:
        """
        Handle errors from async publish operation.

        Logs the error and returns the item to continue pipeline.

        Args:
            failure: Twisted Failure object
            spider: Spider instance for logging
            item: Original item being processed

        Returns:
            The original item (to continue pipeline)
        """
        spider.logger.error(
            f"{self.__class__.__name__}: Publish failed: {failure.getErrorMessage()}",
            exc_info=True
        )
        return item
