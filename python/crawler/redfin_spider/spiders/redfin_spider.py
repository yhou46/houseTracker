import scrapy
import os
from datetime import datetime
from ..items import RedfinPropertyItem
from ..config import ZIP_CODES, REDFIN_ZIP_URL_FORMAT, CITY_URL_MAP
from ..redfin_parser import parse_property_page


class RedfinSpider(scrapy.Spider):
    """
    Sample spider for crawling Redfin property listings.

    This is a basic structure that we'll complete later.
    """

    name = 'redfin_spider'
    allowed_domains = ['redfin.com']

    def __init__(self, *args, **kwargs):
        super(RedfinSpider, self).__init__(*args, **kwargs)
        # Generate start URLs from config
        zip_code_urls = [
            REDFIN_ZIP_URL_FORMAT.format(zip_code=zip_code) for zip_code in ZIP_CODES
        ]
        city_urls = list(CITY_URL_MAP.values())
        self.start_urls = zip_code_urls + city_urls

        # Create debug directory for saving HTML responses
        self.debug_dir = os.path.join(os.path.dirname(__file__), '..', 'debug')
        os.makedirs(self.debug_dir, exist_ok=True)

    async def start(self):
        """Generate initial requests to start the crawling process."""
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results
            )

    def parse_search_results(self, response):
        """
        Parse the search results page to extract property URLs.

        This method:
        1. Extracts property URLs from search results
        2. Follows each property URL
        3. Handles pagination
        """
        self.logger.info(f"Parsing search results from: {response.url}")
        # TODO: page link pattern changed in redfin

        # Save HTML response for debugging
        # self._save_html_response(response, "search_results")

        # Extract property links from search results
        # Using the CSS selector for the property card links
        property_links = response.css('a[data-rf-test-name="basicNode-homeCard"]::attr(href)').getall()

        self.logger.info(f"Found {len(property_links)} property links")

        # Follow each property link
        for i, link in enumerate(property_links, 1):
            if link and '/home/' in link:
                # Convert relative URL to absolute URL
                full_url = response.urljoin(link)
                self.logger.info(f"Property link {i}: {full_url}")

                # Follow the property link to parse individual property page
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_property_page,
                    meta={'original_url': full_url}
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
                    callback=self.parse_search_results
                )
            else:
                self.logger.info(f"No next page found - current page {current_page} is the last page")
        else:
            self.logger.info("No pagination links found - single page results")

    def parse_property_page(self, response):
        """
        Parse individual property page to extract detailed information.
        """
        self.logger.info(f"Parsing property page: {response.url}")

        # Save HTML response for debugging
        # self._save_html_response(response, "property_page")

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

        # Log the extracted data
        self.logger.info(f"Extracted property: {item.get('address', 'Unknown address')}")
        self.logger.info(f"  - Redfin ID: {item.get('redfinId')}")
        self.logger.info(f"  - Area: {item.get('area')} sq ft")

        yield item

    def _extract_property_details(self, response, item):
        """
        Extract property details from the key details section.

        TODO: Implement logic to extract:
        1. Bedrooms and bathrooms
        2. Square footage
        3. Property type
        4. Lot size
        5. Year built
        6. Days on market
        """
        # TODO: Extract bedrooms
        # beds_elem = response.css('...').get()

        # TODO: Extract bathrooms
        # baths_elem = response.css('...').get()

        # TODO: Extract square footage
        # sqft_elem = response.css('...').get()

        # TODO: Extract property type
        # property_type_elem = response.css('...').get()

        # TODO: Extract lot size
        # lot_size_elem = response.css('...').get()

        # TODO: Extract year built
        # year_built_elem = response.css('...').get()

        # TODO: Extract days on market
        # dom_elem = response.css('...').get()

        pass

    def _save_html_response(self, response, page_type):
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

    def closed(self, reason):
        """Called when the spider is closed."""
        self.logger.info(f"Spider {self.name} closed: {reason}")