import unittest
import os
from datetime import datetime

from crawler.redfin_spider.redfin_parser import (
    parse_property_page,
    parse_property_sublinks,
)
from crawler.redfin_spider.test.test_utils import get_html_content_from_url

class TestRedfinParser_ParsePageLinks(unittest.TestCase):
    def setUp(self):
        # Set up any required objects or state before each test
        self.test_sample_dir = os.path.join(os.path.dirname(__file__), "test_samples")

        # Load zip code page from local file for testing
        self.zip_code_page_url = "https://www.redfin.com/zipcode/98109"
        self.zip_code_page_local_path = os.path.join(self.test_sample_dir, "redfin_page_zipcode_98109_20250907_191204.html")
        if os.path.exists(self.zip_code_page_local_path):
            with open(self.zip_code_page_local_path, 'r', encoding='utf-8') as f:
                self.zip_code_page_local_html = f.read()
        else:
            raise FileNotFoundError(f"Test sample file not found: {self.zip_code_page_local_path}")


    def test_find_property_page_links_from_local_file(self):
        # Expected results
        expected_link_count = 41

        links = parse_property_sublinks(self.zip_code_page_local_html)

        # Check link count
        self.assertEqual(
            len(links),
            expected_link_count,
            f"Extracted link count: {len(links)} does not match expected count: {expected_link_count}, local file: {self.zip_code_page_local_path}"
        )

        # Check link format
        is_link_format_correct = all((link.startswith("/WA/Seattle") and link.find("home") != -1) for link in links)
        self.assertTrue(
            is_link_format_correct,
            f"One or more links do not match expected format, local file: {self.zip_code_page_local_path}"
        )

    def test_find_property_page_links_from_url(self):
        # Expected results
        expected_min_link_count = 10

        html_content = get_html_content_from_url(self.zip_code_page_url)
        self.assertIsNotNone(html_content, f"Failed to fetch HTML content from URL: {self.zip_code_page_url}")

        links = parse_property_sublinks(self.zip_code_page_local_html)

        self.assertGreaterEqual(
            len(links),
            expected_min_link_count,
            f"Extracted link count: {len(links)} is less than expected minimum count: {expected_min_link_count}, URL: {self.zip_code_page_url}"
        )

        # Check link format
        is_link_format_correct = all((link.startswith("/WA/Seattle") and link.find("home") != -1) for link in links)
        self.assertTrue(
            is_link_format_correct,
            f"One or more links do not match expected format, local file: {self.zip_code_page_local_path}"
        )

class TestRedfinParser_ParsePropertyPage(unittest.TestCase):
    def setUp(self):
        # Set up any required objects or state before each test
        self.test_sample_dir = os.path.join(os.path.dirname(__file__), "test_samples")

        # Load property page from local file for testing
        self.property_page_url = "https://www.redfin.com/WA/Kenmore/7718-NE-183rd-St-98028/home/282664"
        self.property_page_local_path = os.path.join(self.test_sample_dir, "redfin_page_property_20250907_200312.html")
        if os.path.exists(self.property_page_local_path):
            with open(self.property_page_local_path, 'r', encoding='utf-8') as f:
                self.property_page_local_html = f.read()
        else:
            raise FileNotFoundError(f"Test sample file not found: {self.property_page_local_path}")

    def _validate_property_data(self, property_data: dict):
        # Validate property data
        self.assertEqual(property_data.get("url"), self.property_page_url)
        self.assertEqual(property_data.get("redfinId"), "282664")
        self.assertEqual(property_data.get("address"), "7718 NE 183rd St, Kenmore, WA 98028")
        self.assertEqual(property_data.get("area"), "2000.0 sqft")
        self.assertEqual(property_data.get("propertyType"), "Single-family")
        self.assertEqual(property_data.get("lotArea"), "2.15 acres")
        self.assertEqual(property_data.get("numberOfBedroom"), 3.0)
        self.assertEqual(property_data.get("numberOfBathroom"), 3.0)
        self.assertEqual(property_data.get("yearBuilt"), 1996)
        self.assertEqual(property_data.get("status"), "Sold")
        self.assertEqual(property_data.get("price"), None)
        self.assertEqual(property_data.get("readyToBuildTag"), False)

        # Validate property history
        self.assertEqual(property_data.get("historyCount"), 17)

    def test_parse_property_page_from_local_file(self):
        # Parse property page
        property_data = parse_property_page(self.property_page_url, self.property_page_local_html)

        self._validate_property_data(property_data)


    def test_parse_property_page_from_url(self):
        # Fetch HTML content from URL
        property_html = get_html_content_from_url(self.property_page_url)
        self.assertIsNotNone(property_html, f"Failed to fetch HTML content from URL: {self.property_page_url}")

        # Parse property page
        property_data = parse_property_page(self.property_page_url, property_html)

        # Validate property data
        self._validate_property_data(property_data)



if __name__ == '__main__':
    unittest.main()