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

from shared.aws_s3_util import upload_json_objects, generate_unique_s3_key
from shared.utils import parse_datetime_as_utc

def get_s3_key_prefix_from_json(json_object: Dict[str, Any], worker_id: str) -> str:
        """
        Generate S3 key using data source, date, and worker ID.

        Args:
            json_object: JSON object containing property data
            worker_id: Unique worker identifier

        Returns:
            S3 key in format: {data_source}/{date_str}/{worker_id}.jsonl
        """
        redfin_key = "redfinId"
        is_redfin = redfin_key in json_object
        data_source = None

        if is_redfin:
            data_source = 'redfin'
        else:
            raise ValueError(f"Failed to determine the data source from JSON object. Data source key: {redfin_key} not found.")

        scraped_at = json_object.get("scrapedAt", None)
        date_str = None
        if scraped_at != None:
            date_str = parse_datetime_as_utc(scraped_at).strftime("%Y%m%d")
        else:
            raise ValueError("scrapedAt field is missing in JSON object.")

        if data_source is None or date_str is None:
            raise ValueError(f"Failed to construct S3 key from JSON object. data_source: {data_source}, date_str: {date_str}")

        s3_path = f"{data_source}/{date_str}/{worker_id}"
        return s3_path

class AwsS3Pipeline:
    """
    Pipeline to save scraped items to AWS S3 in batches.

    Items are batched and uploaded to S3 when the threshold is reached.
    Each worker has a unique ID to prevent conflicts when multiple workers run simultaneously.
    When the spider closes, it stores any remaining items to S3.
    """

    # Setting names from crawler
    aws_s3_bucket_name_setting = "AWS_S3_BUCKET_NAME"
    aws_s3_upload_threshold_setting = "AWS_S3_UPLOAD_THRESHOLD"
    aws_region_setting = "AWS_REGION"
    aws_profile_setting = "AWS_PROFILE"
    worker_id_setting = "S3_PIPELINE_WORKER_ID"


    def __init__(self) -> None:
        self.bucket_name: str | None = None
        self.region: str = "us-west-2"
        self.aws_profile: str | None = None
        self.threshold: int = 100
        # Map from s3 key prefix to items
        self._items_map: Dict[str, List[Dict[str, Any]]] = {}
        # Track total items processed
        self.total_items_received: int = 0
        # Track total items uploaded
        self.total_items_uploaded: int = 0
        self.worker_id: str = str(uuid.uuid4())

        self.upload_max_retries = 3

    # Scrapy methods
    @classmethod
    def from_crawler(cls, crawler): # type: ignore[no-untyped-def]
        """
        Create pipeline instance from crawler settings.
        """
        pipeline = cls()

        # Get S3 bucket name from settings (required)
        pipeline.bucket_name = crawler.settings.get(cls.aws_s3_bucket_name_setting)
        if not pipeline.bucket_name:
            raise ValueError(f"{cls.aws_s3_bucket_name_setting} setting is required for S3Pipeline")

        # Get AWS region from settings, default to 'us-west-2'
        pipeline.region = crawler.settings.get(cls.aws_region_setting, 'us-west-2')

        # Get AWS profile from settings (optional)
        pipeline.aws_profile = crawler.settings.get(cls.aws_profile_setting)

        # Get threshold from settings, default to 100 items per zip code
        pipeline.threshold = crawler.settings.getint(cls.aws_s3_upload_threshold_setting, 500)

        worker_id_from_setting = crawler.settings.get(cls.worker_id_setting)
        if worker_id_from_setting is not None:
            pipeline.worker_id = worker_id_from_setting

        return pipeline

    def open_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider opens. Initialize the pipeline.
        """
        if not self.bucket_name:
            raise ValueError("S3 bucket name not set in S3Pipeline")

        spider.logger.info(
            f"S3 Pipeline: Initialized with bucket={self.bucket_name}, "
            f"region={self.region}, threshold={self.threshold}, worker_id={self.worker_id}"
        )

    def close_spider(self, spider): # type: ignore[no-untyped-def]
        """
        Called when spider closes. Upload any remaining items to S3.
        """
        if not self._items_map:
            spider.logger.info("S3 Pipeline: No remaining items to upload")
            return

        spider.logger.info(
            f"S3 Pipeline: Spider closing. Uploading {len(self._items_map)} remaining items..."
        )

        if not self.bucket_name:
            spider.logger.error("S3 Pipeline: Cannot upload - bucket name not set")
            return

        for key, items in self._items_map.items():
            try:
                self._upload_items_batch(
                    key,
                    items,
                    spider,
                )
            except Exception as e:
                spider.logger.error(
                    f"S3 Pipeline: Error uploading remaining items to S3: {e}"
                )
                # Don't raise - we want to log the error but not crash

        # Clear the items batch
        self._items_map.clear()

        spider.logger.info(
            f"S3 Pipeline: Closed. Total items received: {self.total_items_received}, "
            f"Total items uploaded: {self.total_items_uploaded}"
        )

    @staticmethod
    def generate_worker_id(prefix: str) -> str:
        """
        Generate a unique worker ID using prefix, timestamp, and short UUID.

        Args:
            prefix: Prefix for the worker ID (typically spider name)

        Returns:
            Unique worker ID in format: {prefix}_{timestamp}_{short_uuid}
            Example: property_crawler_spider_20260114_063052_a7f3b9c2
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"{prefix}_{timestamp}_{short_uuid}"

    def _get_s3_key_prefix_from_json(self, json_object: Dict[str, Any]) -> str:
        """
        Generate S3 key using data source, date, and worker ID.

        Args:
            json_object: JSON object containing property data
            worker_id: Unique worker identifier

        Returns:
            S3 key in format: {data_source}/{date_str}/{worker_id}.jsonl
        """
        return get_s3_key_prefix_from_json(
            json_object,
            self.worker_id,
        )

    def _upload_items_batch(
            self,
            s3_key_prefix: str,
            items: List[Dict[str, Any]],
            spider: scrapy.Spider,
        ) -> None:
        """
        Upload a batch of items to S3.

        Args:
            items: List of item dictionaries to upload
            spider: The spider instance for logging
        """
        if not self.bucket_name:
            spider.logger.error("S3 Pipeline: Cannot upload - bucket name not set")
            return

        for i in range(self.upload_max_retries):
            try:
                spider.logger.info(
                    f"S3 Pipeline: Uploading batch of {len(items)} items to S3"
                )

                s3_key = generate_unique_s3_key(
                    prefix=s3_key_prefix,
                    extension="jsonl",
                )

                upload_json_objects(
                    json_objects=items,
                    bucket_name=self.bucket_name,
                    s3_key=s3_key,
                    region=self.region,
                    aws_profile=self.aws_profile,
                    overwrite_if_key_exists=False,
                )
                self.total_items_uploaded += len(items)
                spider.logger.info(
                    f"S3 Pipeline: Successfully uploaded {len(items)} items. Retry count: {i+1}"
                )
                return
            except Exception as e:
                spider.logger.error(
                    f"S3 Pipeline: Error uploading batch to S3: {e}, retry count: {i+1} / {self.upload_max_retries}"
                )
                # Don't raise - but let it retry

        # It means the upload failed
        raise Exception(f"S3 Pipeline: Error uploading batch to S3 after retried {self.upload_max_retries} times")

    def process_item(self, item, spider): # type: ignore[no-untyped-def]
        """
        Process each scraped item and add to batch.
        Upload to S3 when threshold is reached.
        """
        try:
            # Convert item to dictionary
            item_dict = ItemAdapter(item).asdict()

            # Add item to batch
            # TODO: It still need to parse records and put records in a dict
            s3_key_prefix = self._get_s3_key_prefix_from_json(
                item_dict,
            )

            if s3_key_prefix not in self._items_map:
                self._items_map[s3_key_prefix] = []

            self._items_map[s3_key_prefix].append(item_dict)
            self.total_items_received += 1

            # Check if threshold is reached
            if len(self._items_map[s3_key_prefix]) >= self.threshold:
                items_to_upload = self._items_map[s3_key_prefix]
                # Clear the batch before uploading
                self._items_map[s3_key_prefix] = []
                # Upload items
                self._upload_items_batch(
                    s3_key_prefix,
                    items_to_upload,
                    spider,
                )

            # Log progress every 50 items
            if self.total_items_received % 50 == 0:
                spider.logger.info(
                    f"S3 Pipeline: received {self.total_items_received} items. "
                    f"Uploaded {self.total_items_uploaded} items. "
                    f"Pending: {len(self._items_map)} items"
                )

            return item

        except Exception as e:
            spider.logger.error(f"S3 Pipeline: Error processing item: {e}")
            # Don't crash the spider, just log the error and continue
            return item