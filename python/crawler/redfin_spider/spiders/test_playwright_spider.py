import scrapy
import os
from datetime import datetime
from scrapy_playwright.page import PageMethod  # type: ignore[import-untyped]

# TODO: can be removed after testing
class TestPlaywrightSpider(scrapy.Spider):
    """
    Test spider to verify Playwright integration with Scrapy.
    This spider will load a Redfin page with JavaScript rendering enabled
    and save the fully rendered HTML to a file for inspection.
    """
    name = 'test_playwright'

    def start_requests(self): # type: ignore[no-untyped-def]
        """Start with a Redfin zipcode page to test JavaScript rendering."""
        test_url = "https://www.redfin.com/zipcode/98109"

        self.logger.info(f"Starting Playwright test with URL: {test_url}")

        yield scrapy.Request(
            url=test_url,
            meta={
                'playwright': True,  # Enable Playwright for this request
                'playwright_include_page': True,  # Include page object for advanced operations
                'playwright_page_methods': [
                    # PageMethod('wait_for_load_state', 'networkidle'),
                    # PageMethod('wait_for_selector', 'div[id="DesktopBlueprintSearchPage__pageContainer"]', timeout=3000),
                    PageMethod('wait_for_timeout', 2000)
                ]
            },
            callback=self.parse,
            errback=self.errback
        )

    def parse(self, response): # type: ignore[no-untyped-def]
        """Parse the rendered page and save HTML for inspection."""
        self.logger.info("Playwright page loaded successfully!")

        # Get the fully rendered HTML content
        rendered_html = response.text

        # Save the rendered HTML to a debug file
        debug_dir = os.path.join(os.path.dirname(__file__), '..', 'debug')
        os.makedirs(debug_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"playwright_test_output_{timestamp}.html"
        filepath = os.path.join(debug_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(rendered_html)

        self.logger.info(f"Rendered HTML saved to: {filepath}")

        # Test basic selectors to see what we can find
        self.test_selectors(response)

        # Log some basic info about the page
        self.log_page_info(response)

    def test_selectors(self, response): # type: ignore[no-untyped-def]
        """Test various selectors to see what content is available."""
        self.logger.info("Testing selectors on rendered page...")

        # Test 1: Look for property cards
        property_cards = response.css('div[data-rf-test-name="basicNode-homeCard"]')
        self.logger.info(f"Found {len(property_cards)} property cards")

        # Test 2: Look for any links containing property URLs
        property_links = response.css('a[href*="/home/"]::attr(href)').getall()
        self.logger.info(f"Found {len(property_links)} property links")

        # Test 3: Look for pagination
        pagination_links = response.css('.PageNumbers__page::attr(href)').getall()
        self.logger.info(f"Found {len(pagination_links)} pagination links")

        # Test 4: Look for any links at all
        all_links = response.css('a::attr(href)').getall()
        self.logger.info(f"Found {len(all_links)} total links")

        # Test 5: Look for specific text content
        page_title = response.css('title::text').get()
        self.logger.info(f"Page title: {page_title}")

        # Test 6: Look for property addresses
        addresses = response.css('address::text').getall()
        self.logger.info(f"Found {len(addresses)} address elements")

        # Test 7: Look for map home cards specifically
        map_cards = response.css('div[data-rf-test-name="MapHomeCard"]')
        self.logger.info(f"Found {len(map_cards)} map home cards")

        # Test 8: Look for any elements with property IDs in href
        property_id_links = response.css('a[href*="40065234"]')
        self.logger.info(f"Found {len(property_id_links)} links with specific property ID 40065234")

        # Log first few results for inspection
        if property_links:
            self.logger.info(f"First 3 property links: {property_links[:3]}")
        if all_links:
            self.logger.info(f"First 5 total links: {all_links[:5]}")

    def log_page_info(self, response): # type: ignore[no-untyped-def]
        """Log basic information about the rendered page."""
        self.logger.info("=== PAGE INFORMATION ===")
        self.logger.info(f"Response URL: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        self.logger.info(f"HTML length: {len(response.text)} characters")

        # Check if we have JavaScript-rendered content
        if 'data-rf-test-name="basicNode-homeCard"' in response.text:
            self.logger.info("✅ JavaScript rendering successful - property cards found!")
        else:
            self.logger.warning("❌ JavaScript rendering may have failed - no property cards found")

        # Check for common Redfin elements
        redfin_elements = [
            'data-rf-test-name',
            'bp-Homecard',
            'MapHomeCard',
            'property-card'
        ]

        found_elements = []
        for element in redfin_elements:
            if element in response.text:
                found_elements.append(element)

        self.logger.info(f"Found Redfin elements: {found_elements}")

        # Check for the specific property link mentioned by the user
        if '40065234' in response.text:
            self.logger.info("✅ Found property ID 40065234 in the HTML!")
        else:
            self.logger.warning("❌ Property ID 40065234 not found in the HTML")

        # Check for React-related content
        if 'react' in response.text.lower() or 'react' in response.css('script::attr(src)').getall():
            self.logger.info("✅ React-related content detected")
        else:
            self.logger.info("ℹ️ No obvious React content detected")

    def errback(self, failure):
        """Handle any errors during the request."""
        self.logger.error(f"Playwright test failed: {failure.value}")
        self.logger.error(f"Failure type: {type(failure.value).__name__}")

        # Log the full traceback for debugging
        if hasattr(failure.value, '__traceback__'):
            import traceback
            self.logger.error(f"Traceback: {traceback.format_tb(failure.value.__traceback__)}")
