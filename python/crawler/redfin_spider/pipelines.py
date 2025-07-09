# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
from datetime import datetime
from itemadapter import ItemAdapter


class RedfinSpiderPipeline:
    def process_item(self, item, spider):
        return item


class JsonlPipeline:
    """
    Pipeline to save scraped items to a JSONL file.

    Each item is written as a single JSON line to the output file.
    """

    def __init__(self):
        self.output_file = None
        self.output_dir = None

    @classmethod
    def from_crawler(cls, crawler):
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

    def open_spider(self, spider):
        """
        Called when spider opens. Create output directory and file.
        """
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Full path to output file
        self.filepath = os.path.join(self.output_dir, self.output_file)

        # Open file for writing
        self.file = open(self.filepath, 'w', encoding='utf-8')

        spider.logger.info(f"JSONL Pipeline: Output file opened at {self.filepath}")

    def close_spider(self, spider):
        """
        Called when spider closes. Close the output file.
        """
        if hasattr(self, 'file') and self.file:
            self.file.close()
            spider.logger.info(f"JSONL Pipeline: Output file closed. Total records written: {getattr(self, 'record_count', 0)}")

    def process_item(self, item, spider):
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
