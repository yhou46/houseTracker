# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Self, cast
from itemadapter import ItemAdapter

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
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"redfin_properties_{timestamp}.jsonl"

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


class AwsS3Pipeline:
    """
    Pipeline to save scraped items to AWS S3, grouped by zip code.

    Items are stored with different keys based on zip code. The pipeline counts
    the number of items per zip code and stores them to S3 when a threshold is reached.
    When the spider closes, it stores any remaining items to S3.
    """

    def __init__(self) -> None:
        self.bucket_name: str | None = None
        self.region: str = "us-west-2"
        self.aws_profile: str | None = None
        self.threshold: int = 100
        # Dictionary to store items grouped by zip code
        self.items_by_zip: Dict[str, List[Dict[str, Any]]] = {}
        # Track total items processed
        self.total_items_received: int = 0
        # Track total items uploaded
        self.total_items_uploaded: int = 0

    @classmethod
    def from_crawler(cls, crawler): # type: ignore[no-untyped-def]
        """
        Create pipeline instance from crawler settings.
        """
        pipeline = cls()

        # Get S3 bucket name from settings (required)
        pipeline.bucket_name = crawler.settings.get('AWS_S3_BUCKET_NAME')
        if not pipeline.bucket_name:
            raise ValueError("AWS_S3_BUCKET_NAME setting is required for S3Pipeline")

        # Get AWS region from settings, default to 'us-west-2'
        pipeline.region = crawler.settings.get('AWS_REGION', 'us-west-2')

        # Get AWS profile from settings (optional)
        pipeline.aws_profile = crawler.settings.get('AWS_PROFILE')

        # Get threshold from settings, default to 100 items per zip code
        pipeline.threshold = crawler.settings.getint('S3_UPLOAD_THRESHOLD', 100)

        return pipeline

    def open_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider opens. Initialize the pipeline.
        """
        if not self.bucket_name:
            raise ValueError("S3 bucket name not set in S3Pipeline")

        spider.logger.info(
            f"S3 Pipeline: Initialized with bucket={self.bucket_name}, "
            f"region={self.region}, threshold={self.threshold}"
        )

    def close_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider closes. Upload any remaining items to S3.
        """
        if not self.items_by_zip:
            spider.logger.info("S3 Pipeline: No remaining items to upload")
            return

        spider.logger.info(
            f"S3 Pipeline: Spider closing. Uploading {len(self.items_by_zip)} "
            f"zip code groups with remaining items..."
        )

        # Upload all remaining items
        remaining_items: List[Dict[str, Any]] = []
        for zip_code, items in self.items_by_zip.items():
            remaining_items.extend(items)
            spider.logger.info(
                f"S3 Pipeline: Uploading {len(items)} remaining items for zip code {zip_code}"
            )

        if remaining_items:
            if not self.bucket_name:
                spider.logger.error("S3 Pipeline: Cannot upload - bucket name not set")
                return
            try:
                upload_json_objects(
                    json_objects=remaining_items,
                    bucket_name=self.bucket_name,
                    region=self.region,
                    aws_profile=self.aws_profile,
                    continue_if_key_exists=True,
                )
                self.total_items_uploaded += len(remaining_items)
                spider.logger.info(
                    f"S3 Pipeline: Successfully uploaded {len(remaining_items)} remaining items to S3"
                )
            except Exception as e:
                spider.logger.error(
                    f"S3 Pipeline: Error uploading remaining items to S3: {e}"
                )
                # Don't raise - we want to log the error but not crash

        # Clear the items dictionary
        self.items_by_zip.clear()

        spider.logger.info(
            f"S3 Pipeline: Closed. Total items processed: {self.total_items_received}, "
            f"Total items uploaded: {self.total_items_uploaded}"
        )

    def _upload_items_for_zipcode(self, zip_code: str, items: List[Dict[str, Any]], spider): # type: ignore[no-untyped-def]
        """
        Upload items for a specific zip code to S3.

        Args:
            zip_code: The zip code for the items
            items: List of item dictionaries to upload
            spider: The spider instance for logging
        """
        if not self.bucket_name:
            spider.logger.error("S3 Pipeline: Cannot upload - bucket name not set")
            return
        try:
            spider.logger.info(
                f"S3 Pipeline: Uploading {len(items)} items for zip code {zip_code} to S3"
            )
            upload_json_objects(
                json_objects=items,
                bucket_name=self.bucket_name,
                region=self.region,
                aws_profile=self.aws_profile,
                continue_if_key_exists=True,
            )
            self.total_items_uploaded += len(items)
            spider.logger.info(
                f"S3 Pipeline: Successfully uploaded {len(items)} items for zip code {zip_code}"
            )
        except Exception as e:
            spider.logger.error(
                f"S3 Pipeline: Error uploading items for zip code {zip_code} to S3: {e}"
            )
            # Don't raise - we want to log the error but not crash the spider

    def process_item(self, item, spider): # type: ignore[no-untyped-def]
        """
        Process each scraped item and group it by zip code.
        Upload to S3 when threshold is reached for a zip code.
        """
        try:
            # Convert item to dictionary
            item_dict = ItemAdapter(item).asdict()

            # Get zip code from item
            zip_code = item_dict.get('zipCode')
            if not zip_code:
                spider.logger.error(
                    "S3 Pipeline: Item missing zipCode field, dropping item"
                )
                raise DropItem(f"Item missing zipCode field: {item_dict.get('url', 'unknown URL')}")

            # Add item to the appropriate zip code group
            if zip_code not in self.items_by_zip:
                self.items_by_zip[zip_code] = []

            self.items_by_zip[zip_code].append(item_dict)
            self.total_items_received += 1

            # Check if threshold is reached for this zip code
            if len(self.items_by_zip[zip_code]) >= self.threshold:
                items_to_upload = self.items_by_zip[zip_code]
                # Clear the list for this zip code before uploading
                self.items_by_zip[zip_code] = []
                # Upload items
                self._upload_items_for_zipcode(zip_code, items_to_upload, spider)

            # Log progress every 50 items
            if self.total_items_received % 50 == 0:
                spider.logger.info(
                    f"S3 Pipeline: received {self.total_items_received} items. "
                    f"Uploaded {self.total_items_uploaded} items. "
                    f"Pending: {sum(len(items) for items in self.items_by_zip.values())} items"
                )

            return item

        except Exception as e:
            spider.logger.error(f"S3 Pipeline: Error processing item: {e}")
            # Don't crash the spider, just log the error and continue
            return item


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
        Called when spider closes. Flush any remaining items in batch.
        """
        pipeline_name = self.__class__.__name__

        # Publish any remaining items in batch
        if self.batch:
            spider.logger.info(
                f"{pipeline_name}: Flushing {len(self.batch)} remaining items"
            )
            batch_to_publish = self.batch.copy()
            self.batch = []

            # Use deferred_from_coro to bridge asyncio coroutine to Twisted Deferred
            # This works with AsyncioSelectorReactor
            deferred = deferred_from_coro(self._publish_batch_async(batch_to_publish, spider))

            # Add callback to handle completion
            def handle_close_result(result): # type: ignore[no-untyped-def]
                spider.logger.info(f"{pipeline_name}: Flush completed")
                return result

            def handle_close_error(failure): # type: ignore[no-untyped-def]
                spider.logger.error(f"{pipeline_name}: Flush failed: {failure.getErrorMessage()}")
                return failure

            deferred.addCallback(handle_close_result)
            deferred.addErrback(handle_close_error)

        # Close Redis connection
        if self.redis_client:
            spider.logger.info(f"{pipeline_name}: Closing Redis connection")
            close_deferred = deferred_from_coro(self.redis_client.aclose())

            def handle_close_conn_result(result): # type: ignore[no-untyped-def]
                spider.logger.info(f"{pipeline_name}: Redis connection closed")
                return result

            close_deferred.addCallback(handle_close_conn_result)

        spider.logger.info(
            f"{pipeline_name}: Closed. "
            f"Total published: {self.total_published}, "
            f"Total failed: {self.total_failed}"
        )

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
    default_batch_size = 50  # Smaller batch size since property data is larger

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
