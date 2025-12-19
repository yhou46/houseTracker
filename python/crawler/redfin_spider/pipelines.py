# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

# houseTracker imports
from data_service.aws_s3_util import upload_json_objects





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
