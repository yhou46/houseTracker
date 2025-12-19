import os
from datetime import datetime
from typing import (
    Any,
    Dict,
)

import scrapy
from scrapy.http import Response, Request
from scrapy.exceptions import DropItem

from ..items import RedfinPropertyItem
from .monolith_config import (
    ZIP_CODES,
    REDFIN_ZIP_URL_FORMAT,
    CITY_URL_MAP,
    ENABLE_BROWSER_RENDERING,
    AWS_S3_BUCKET_NAME,
    AWS_REGION,
    )
from ..redfin_parser import parse_property_page, parse_property_sublinks
from shared.logger_factory import configure_logger

class RedfinSpiderMonolith(scrapy.Spider):
    """
    Sample spider for crawling Redfin property listings.

    This is a basic structure that we'll complete later.
    """

    name = "redfin_spider_monolith"
    allowed_domains = [
        "redfin.com",
    ]

    custom_settings = {
        # Logging settings
        "LOG_LEVEL": 'INFO',
        "LOG_STDOUT": True,
        "LOG_FORMAT": '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        "LOG_DATEFORMAT": '%Y-%m-%d %H:%M:%S',
        "LOG_FILE_APPEND": False,

        # Crawler settings
        "BOT_NAME": "redfin_spider_monolith",
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 2,

        # Pipelines
        "ITEM_PIPELINES": {
            "redfin_spider.pipelines.JsonlPipeline": 300,
            "redfin_spider.pipelines.AwsS3Pipeline": 301,
        },

        # Playwright specific settings
        # It is not working yet...
        # Reference: https://github.com/scrapy-plugins/scrapy-playwright?tab=readme-ov-file#supported-settings
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        # Playwright-specific settings
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,  # Run browser in background (no visible window)
            "timeout": 20 * 1000,  # 20 seconds timeout
        },
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,  # 30 seconds for page load

        # Customized settings
        # "JSONL_OUTPUT_DIR": "redfin_output",
        "JSONL_OUTPUT_FILE": None, # Will use timestamp-based filename if None. Will be overridden later
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs): # type: ignore[no-untyped-def]
        spider = super().from_crawler(crawler, *args, **kwargs)

        # Create log directory if not exists
        log_directory = os.path.join(os.path.dirname(__file__), "..", f"{cls.name}_logs")
        os.makedirs(log_directory, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
        # Set up log path
        spider.settings.set(
            "LOG_FILE", os.path.join(log_directory, f'{cls.name}_{timestamp}.log'), priority="spider",
        )

        # Set up output directory
        output_directory = os.path.join(os.path.dirname(__file__), "..", f"{cls.name}_output")
        os.makedirs(output_directory, exist_ok=True)
        spider.settings.set(
            "JSONL_OUTPUT_DIR", output_directory, priority="spider",
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"redfin_properties_{timestamp}.jsonl"
        spider.settings.set(
            "JSONL_OUTPUT_FILE", output_filename, priority="spider",
        )

        # Set up AWS S3 settings
        spider.settings.set(
            "AWS_S3_BUCKET_NAME", AWS_S3_BUCKET_NAME, priority="spider",
        )
        spider.settings.set(
            "AWS_REGION", AWS_REGION, priority="spider",
        )

        return spider

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Generate start URLs from config
        zip_code_urls = [
            REDFIN_ZIP_URL_FORMAT.format(zip_code=zip_code) for zip_code in ZIP_CODES
        ]
        city_urls = list(CITY_URL_MAP.values())
        self.start_urls = zip_code_urls + city_urls

        # # Create log directory if not exists
        # log_directory = os.path.join(os.path.dirname(__file__), "..", f"{self.name}_logs")
        # os.makedirs(log_directory, exist_ok=True)
        # timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
        # # Set up log path
        # type(self).custom_settings["LOG_FILE"] = os.path.join(log_directory, f'{self.name}_{timestamp}.log')

        # Create debug directory for saving HTML responses
        self.debug_dir = os.path.join(os.path.dirname(__file__), '..', 'debug')
        os.makedirs(self.debug_dir, exist_ok=True)

        # Configure logger for other functions
        configure_logger(
            logger_override=self.logger
        )

    async def start(self): # type: ignore[no-untyped-def]
        """Generate initial requests to start the crawling process."""
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results, # type: ignore[arg-type]
                meta={
                    'original_url': url,
                    'playwright': ENABLE_BROWSER_RENDERING,
                    },
            )

    def parse_search_results(self, response: Response): # type: ignore[no-untyped-def]
        """
        Parse the search results page to extract property URLs.

        This method:
        1. Extracts property URLs from search results
        2. Follows each property URL
        3. Handles pagination
        """
        self.logger.info(f"Parsing search results from: {response.url}")

        # Extract zip code from URL if this is a zip code search page
        # e.g., https://www.redfin.com/zipcode/98109
        zip_code = None
        if '/zipcode/' in response.url:
            try:
                zip_code = response.url.split('/zipcode/')[1].split('/')[0]
                self.logger.info(f"Extracted zip code from URL: {zip_code}")
            except (IndexError, ValueError):
                self.logger.warning(f"Failed to extract zip code from URL: {response.url}")

        # Also check if zip_code was passed from previous request (for pagination)
        if not zip_code:
            zip_code = response.meta.get('zip_code')

        # Extract property links from search results
        property_links = parse_property_sublinks(response.text)

        self.logger.info(f"Found {len(property_links)} property links")

        # Follow each property link
        for i, link in enumerate(property_links, 1):
            if link and '/home/' in link:
                # Convert relative URL to absolute URL
                full_url = response.urljoin(link)
                self.logger.info(f"Property link {i}: {full_url}")

                # Follow the property link to parse individual property page
                # Pass zip_code through meta so it's available in parse_property_page
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_property_page, # type: ignore[arg-type]
                    meta={
                        'original_url': full_url,
                        'zip_code': zip_code  # Pass zip code through meta
                    }
                )
            else:
                self.logger.warning(f"Skipping invalid link {i}: {link}")

        # Handle pagination - extract all page links from the pagination section
        pagination_links = response.css('.PageNumbers__page::attr(href)').getall()

        if pagination_links:
            self.logger.info(f"Found {len(pagination_links)} pagination links: {pagination_links}")

            # Get the current page number from the URL
            current_url = response.url
            current_page = 1  # Default to page 1

            # Extract current page number from URL if it exists
            if '/page-' in current_url:
                try:
                    current_page = int(current_url.split('/page-')[-1])
                except ValueError:
                    current_page = 1

            self.logger.info(f"Current page: {current_page}")

            # Find the next page link
            next_page = None
            for link in pagination_links:
                if f'/page-{current_page + 1}' in link:
                    next_page = link
                    break

            if next_page:
                next_url = response.urljoin(next_page)
                self.logger.info(f"Following next page: {next_url}")
                yield scrapy.Request(
                    url=next_url,
                    callback=self.parse_search_results, # type: ignore[arg-type]
                    meta={'zip_code': zip_code}  # Preserve zip code for pagination
                )
            else:
                self.logger.info(f"No next page found - current page {current_page} is the last page")
        else:
            self.logger.info("No pagination links found - single page results")

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
        except Exception as error:
            self.logger.error(f"Failed to parse property page {response.url}: {error}")
            raise DropItem(f"Failed to parse property page {response.url}: {error}")

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

        # Log the extracted data
        self.logger.info(f"Extracted property: {item.get('address', 'Unknown address')}")
        self.logger.info(f"  - Redfin ID: {item.get('redfinId')}")
        self.logger.info(f"  - Area: {item.get('area')} sq ft")
        self.logger.info(f"  - Zip Code: {item.get('zipCode')}")

        yield item

    def _save_html_response(self, response, page_type): # type: ignore
        """
        Save HTML response to a file for debugging purposes.

        Args:
            response: Scrapy response object
            page_type: Type of page (e.g., 'search_results', 'property_page')
        """
        try:
            # Create filename with timestamp and page type
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{page_type}_{timestamp}.html"
            filepath = os.path.join(self.debug_dir, filename)

            # Save the HTML content
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            self.logger.info(f"Saved HTML response to: {filepath}")

        except Exception as e:
            self.logger.error(f"Failed to save HTML response: {e}")

    def closed(self, reason): # type: ignore
        """Called when the spider is closed."""
        self.logger.info(f"Spider {self.name} closed: {reason}")