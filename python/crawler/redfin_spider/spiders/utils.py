"""
Shared utility functions for Redfin spiders.

This module contains reusable logic extracted from monolith_spider.py
to support URL Discovery Spider, Property Crawler Spider, and future spiders.
"""
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Any, cast
from scrapy.http import Response
import logging

from ..redfin_parser import parse_property_sublinks
from shared.logger_factory import LoggerLike


# =====================================
# Configuration & Setup
# =====================================

def load_json_config(config_path: str) -> dict[str, Any]:
    """
    Load and parse JSON configuration file.

    Args:
        config_path: Path to JSON config file

    Returns:
        Parsed configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    return cast(dict[str, Any], config)


def setup_spider_logging(
    spider_name: str,
    base_directory: str
) -> str:
    """
    Create log directory and return log file path.

    Args:
        spider_name: Name of the spider (used in filename)
        base_directory: Base directory where log folder will be created

    Returns:
        Full path to log file

    Example:
        >>> setup_spider_logging("property_url_discovery", "/app/spiders")
        "/app/spiders/../property_url_discovery_logs/property_url_discovery_20260107_123456.log"
    """
    log_directory = os.path.join(base_directory, "..", f"{spider_name}_logs")
    os.makedirs(log_directory, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{spider_name}_{timestamp}.log"
    log_file_path = os.path.join(log_directory, log_filename)

    return log_file_path

# TODO: not used and have bugs since file is not unique
def setup_output_directory(
    spider_name: str,
    base_directory: str,
    file_prefix: str = "output"
) -> Tuple[str, str]:
    """
    Create output directory and generate timestamped filename.

    Args:
        spider_name: Name of the spider (used in directory name)
        base_directory: Base directory where output folder will be created
        file_prefix: Prefix for output filename (default: "output")

    Returns:
        Tuple of (output_directory_path, output_filename)

    Example:
        >>> setup_output_directory("property_url_discovery", "/app/spiders", "urls")
        ("/app/spiders/../property_url_discovery_output", "urls_20260107_123456.jsonl")
    """
    output_directory = os.path.join(base_directory, "..", f"{spider_name}_output")
    os.makedirs(output_directory, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{file_prefix}_{timestamp}.jsonl"

    return output_directory, output_filename

# TODO: removed it? since it is no used
def create_debug_directory(base_directory: str) -> str:
    """
    Create and return debug directory path.

    Args:
        base_directory: Base directory where debug folder will be created

    Returns:
        Path to debug directory

    Example:
        >>> create_debug_directory("/app/spiders")
        "/app/spiders/../debug"
    """
    debug_dir = os.path.join(base_directory, '..', 'debug')
    os.makedirs(debug_dir, exist_ok=True)
    return debug_dir


# =====================================
# URL Parsing & Extraction
# =====================================

# TODO: not used. removed later
def extract_zip_code_from_url(url: str, meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Extract zip code from URL or request metadata.

    Tries two methods:
    1. Parse from URL path (e.g., https://www.redfin.com/zipcode/98109)
    2. Check request meta dictionary

    Args:
        url: The URL to parse
        meta: Optional Scrapy request meta dictionary

    Returns:
        Zip code string or None if not found

    Example:
        >>> extract_zip_code_from_url("https://www.redfin.com/zipcode/98109")
        "98109"
        >>> extract_zip_code_from_url("https://www.redfin.com/city/Seattle", {"zip_code": "98101"})
        "98101"
    """
    zip_code = None

    # Method 1: Extract from URL path
    if '/zipcode/' in url:
        try:
            # Split by '/zipcode/' and take the next segment
            zip_code = url.split('/zipcode/')[1].split('/')[0]
        except (IndexError, ValueError):
            # Failed to parse - will try meta next
            pass

    # Method 2: Check meta dictionary
    if not zip_code and meta:
        zip_code = meta.get('zip_code')

    return zip_code


def extract_property_urls_from_response(
    response: Response,
    logger: LoggerLike,
) -> List[str]:
    """
    Extract and validate property URLs from search results.

    Uses parse_property_sublinks() to extract links, then:
    - Filters for valid property URLs (must contain '/home/')
    - Converts relative URLs to absolute URLs
    - Returns list of validated absolute URLs

    Args:
        response: Scrapy Response object from search results page
        logger: Logger instance for logging warnings

    Returns:
        List of absolute property URLs

    Example:
        >>> urls = extract_property_urls_from_response(response, logger)
        >>> urls
        ['https://www.redfin.com/WA/Seattle/.../home/123', ...]
    """
    # Extract property links from HTML
    property_links = parse_property_sublinks(response.text)

    logger.info(f"Found {len(property_links)} property links in raw HTML")

    # Filter and convert to absolute URLs
    valid_urls = []
    for i, link in enumerate(property_links, 1):
        if link and '/home/' in link:
            # Convert relative URL to absolute URL
            full_url = response.urljoin(link)
            valid_urls.append(full_url)
            logger.debug(f"Property link {i}: {full_url}")
        else:
            logger.warning(f"Skipping invalid link {i}: {link}")

    logger.info(f"Extracted {len(valid_urls)} valid property URLs")

    return valid_urls


def generate_start_urls_from_config(config: Dict[str, Any]) -> List[str]:
    """
    Generate Redfin start URLs from configuration structure.

    Expects config with structure:
    {
        "start_urls": {
            "zip_codes": ["98109", "98052"],
            "cities": [
                {"city": "Redmond", "state": "WA", "url": "/city/14913/WA/Redmond"},
                ...
            ]
        }
    }

    Args:
        config: Configuration dictionary

    Returns:
        List of absolute Redfin URLs

    Raises:
        KeyError: If required config keys are missing

    Example:
        >>> config = {"start_urls": {"zip_codes": ["98109"], "cities": []}}
        >>> generate_start_urls_from_config(config)
        ['https://www.redfin.com/zipcode/98109']
    """
    REDFIN_BASE_URL = "https://www.redfin.com"
    REDFIN_ZIP_URL_FORMAT = "https://www.redfin.com/zipcode/{zip_code}"

    start_urls = []

    # Get start_urls section from config
    start_urls_config = config.get('start_urls', {})

    # Generate URLs from zip codes
    zip_codes = start_urls_config.get('zip_codes', [])
    for zip_code in zip_codes:
        url = REDFIN_ZIP_URL_FORMAT.format(zip_code=zip_code)
        start_urls.append(url)

    # Generate URLs from cities
    cities = start_urls_config.get('cities', [])
    for city_entry in cities:
        # city_entry: {"city": "Redmond", "state": "WA", "url": "/city/14913/WA/Redmond"}
        relative_url = city_entry.get('url')
        if relative_url:
            # Convert relative URL to absolute
            if relative_url.startswith('/'):
                absolute_url = REDFIN_BASE_URL + relative_url
            else:
                absolute_url = REDFIN_BASE_URL + '/' + relative_url
            start_urls.append(absolute_url)

    return start_urls


# =====================================
# Pagination
# =====================================

def extract_current_page_number(url: str) -> int:
    """
    Extract current page number from URL.

    Looks for '/page-N' pattern in URL. Returns 1 if no page number found.

    Args:
        url: URL to parse

    Returns:
        Page number (1-indexed)

    Example:
        >>> extract_current_page_number("https://www.redfin.com/zipcode/98109/page-3")
        3
        >>> extract_current_page_number("https://www.redfin.com/zipcode/98109")
        1
    """
    current_page = 1  # Default to page 1

    if '/page-' in url:
        try:
            # Extract the page number after '/page-'
            page_str = url.split('/page-')[-1]
            # Handle cases where there might be more path segments after page number
            page_str = page_str.split('/')[0]
            current_page = int(page_str)
        except (ValueError, IndexError):
            # If parsing fails, default to page 1
            current_page = 1

    return current_page


def find_next_pagination_link(
    response: Response,
    current_page: int,
    logger: LoggerLike,
) -> Optional[str]:
    """
    Find next page URL from pagination links.

    Extracts pagination links from page using CSS selector '.PageNumbers__page::attr(href)',
    then finds the link for current_page + 1.

    Args:
        response: Scrapy Response object
        current_page: Current page number
        logger: Logger instance for logging

    Returns:
        Absolute URL to next page, or None if no next page exists

    Example:
        >>> next_url = find_next_pagination_link(response, 2, logger)
        >>> next_url
        'https://www.redfin.com/zipcode/98109/page-3'
    """
    # Extract all pagination links from the page
    pagination_links = response.css('.PageNumbers__page::attr(href)').getall()

    if not pagination_links:
        logger.info("No pagination links found - single page results")
        return None

    logger.info(f"Found {len(pagination_links)} pagination links on page {current_page}")
    logger.debug(f"Pagination links: {pagination_links}")

    # Find the next page link (current_page + 1)
    next_page_link = None
    next_page_number = current_page + 1

    for link in pagination_links:
        if f'/page-{next_page_number}' in link:
            next_page_link = link
            break

    if next_page_link:
        # Convert relative URL to absolute URL
        next_url = response.urljoin(next_page_link)
        logger.info(f"Found next page: {next_url}")
        return next_url
    else:
        logger.info(f"No next page found - page {current_page} is the last page")
        return None


# =====================================
# Debugging
# =====================================

# TODO: remove later? Not used
def save_html_response_debug(
    response: Response,
    page_type: str,
    debug_dir: str,
    logger: logging.Logger
) -> None:
    """
    Save HTML response to file for debugging purposes.

    Creates a timestamped HTML file in the debug directory.

    Args:
        response: Scrapy Response object
        page_type: Type of page (e.g., 'search_results', 'property_page')
        debug_dir: Directory where HTML file will be saved
        logger: Logger instance for logging

    Returns:
        None

    Example:
        >>> save_html_response_debug(response, "search_results", "/app/debug", logger)
        # Creates file: /app/debug/search_results_20260107_123456.html
    """
    try:
        # Create filename with timestamp and page type
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{page_type}_{timestamp}.html"
        filepath = os.path.join(debug_dir, filename)

        # Save the HTML content
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)

        logger.info(f"Saved HTML response to: {filepath}")

    except Exception as e:
        logger.error(f"Failed to save HTML response: {e}", exc_info=True)
