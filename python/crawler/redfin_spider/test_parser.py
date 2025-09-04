#!/usr/bin/env python3
"""
Test script for the Redfin property parser.
Uses saved HTML files to test the parsing logic.
"""

import os
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import json

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from crawler.redfin_spider.redfin_parser import parse_property_page, parse_property_history, _parse_history_from_javascript, parse_property_links

def test_parser_with_saved_file(filename: str):
    """Test the parser using a saved HTML file."""

    # Use the most recent property page file
    debug_dir = Path(__file__).parent / "./debug/"
    print(debug_dir)
    property_files = list(debug_dir.glob("*.html"))

    if not property_files:
        print("No property page files found in debug directory")
        return

    # Use the most recent file (sorted by modification time)
    latest_file = Path.joinpath(debug_dir, filename)
    print(f"Testing with file: {latest_file.name}")

    # Read the HTML content
    with open(latest_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Create a sample URL (since we don't have the original URL)
    sample_url = "https://www.redfin.com/WA/Kirkland/11095-Champagne-Point-Rd-NE-98034/home/280166"

    # Test the parser
    print("\n" + "="*50)
    print("TESTING PARSER")
    print("="*50)

    try:
        # result = parse_property_page(
        #      url=sample_url,
        #      html_content=html_content,
        #      spider_name="test_spider"
        # )

        # print("\nPARSED RESULT:")
        # print("-" * 30)
        # for key, value in result.items():
        #     print(f"{key}: {value}")

        links = parse_property_links(html_content)
        print(f"Extracted {len(links)} property links:")
        for link in links:
            print(link)


    except Exception as e:
        print(f"Error during parsing: {e}")
        import traceback
        traceback.print_exc()

def test_parser_with_url(url: str):
    """
    Test the parser by fetching a page from URL and parsing it.

    Args:
        url: Redfin property URL to test
    """
    print(f"\n" + "="*50)
    print(f"TESTING PARSER WITH URL: {url}")
    print("="*50)

    try:
        # Fetch the page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print("✓ Page fetched successfully!")
            html_content = response.text

            # Save the page to debug folder
            debug_dir = Path(__file__).parent / "debug"
            debug_dir.mkdir(exist_ok=True)

            # Create filename with timestamp and URL info
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Extract property ID from URL for filename
            url_parts = url.split('/')
            property_id = url_parts[-1] if url_parts else "unknown"
            filename = f"property_page_{property_id}_{timestamp}.html"
            filepath = debug_dir / filename

            # Save the HTML content
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"✓ Page saved to: {filepath}")

            # Test the parser
            # result = parse_property_page(
            #     url=url,
            #     html_content=html_content,
            #     spider_name="test_spider"
            # )

            # print("\nPARSED RESULT:")
            # print("-" * 30)
            # for key, value in result.items():
            #     if key == 'history':
            #         print(f"{key}: {len(value)} events")
            #         # Print first few history events
            #         for i, event in enumerate(value[:3]):
            #             print(f"  Event {i+1}: {event}")
            #         if len(value) > 3:
            #             print(f"  ... and {len(value) - 3} more events")
            #     else:
            #         print(f"{key}: {value}")
            links = parse_property_links(html_content)
            print(f"Extracted {len(links)} property links:")
            for link in links:
                print(link)

        else:
            print(f"✗ Failed to fetch page: {response.status_code}")

    except Exception as e:
        print(f"Error during parsing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Test with saved file
    # filename = "zipcode_page_20250830.html"
    # test_parser_with_saved_file(filename)

    # Test with live URL (uncomment to test)
    test_parser_with_url("https://www.redfin.com/zipcode/98109")