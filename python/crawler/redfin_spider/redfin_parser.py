"""
Redfin property page parser module.

This module contains functions to parse Redfin property pages and extract
property information. It can be used by both the spider and unit tests.
"""

import re
from datetime import datetime
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup
import json

# Set up logging
import logging
logger = logging.getLogger(__name__)

def extract_redfin_id(url: str) -> Optional[str]:
    """
    Extract Redfin ID from a property URL.
    
    Args:
        url: Redfin property URL (e.g., "https://www.redfin.com/WA/Redmond/11594-174th-Ct-NE-98052/home/22497318")
    
    Returns:
        Redfin ID (e.g., "22497318") or None if not found
    """
    url_parts = url.split('/')
    if 'home' in url_parts:
        home_index = url_parts.index('home')
        if len(url_parts) > home_index + 1:
            return url_parts[home_index + 1]
    return None

# Parse metadata from meta tags
def parse_meta_tag(beautiful_soup: BeautifulSoup ,name: str) -> Optional[str]:
    meta_tag = beautiful_soup.find("meta", attrs={"name": name})
    if meta_tag and hasattr(meta_tag, 'attrs'):
        attrs = meta_tag.attrs
        if isinstance(attrs, dict) and "content" in attrs:
            return str(attrs["content"])
    return None

def parse_property_details(html_content: str) -> Dict[str, Any]:
    """
    Parse property details from HTML content using Beautiful Soup.
    
    Args:
        html_content: HTML content of the property page
    
    Returns:
        Dictionary containing extracted property details and history
    """
    soup = BeautifulSoup(html_content, "html.parser")
    result: Dict[str, Any] = {
        'address': None,
        'area':  None,
        'propertyType': None,
        'lotArea': None,
        'numberOfBedroom': None,
        'numberOfBathroom': None,
        'yearBuilt': None,
        'status': None,
    }
    
    # Parse address from title
    title = soup.title
    if title and title.string:
        parts = title.string.split("|")
        if len(parts) >= 2:
            result['address'] = parts[0].strip()
    
    # Parse bedrooms and bathrooms
    beds = parse_meta_tag(soup, "twitter:text:beds")
    if beds:
        try:
            result['numberOfBedroom'] = float(beds)
        except ValueError:
            result['numberOfBedroom'] = None
    
    baths = parse_meta_tag(soup, "twitter:text:baths")
    if baths:
        try:
            result['numberOfBathroom'] = float(baths)
        except ValueError:
            result['numberOfBathroom'] = None
    
    # Parse square footage
    sqft = parse_meta_tag(soup, "twitter:text:sqft")
    if sqft:
        sqft_clean = sqft.replace(",", "")
        try:
            # Extract number and unit
            parts = sqft_clean.split(" ")
            if len(parts) >= 2:
                number = float(parts[0])
                unit = "sqft"
                result['area'] = f"{number} {unit}"
            else:
                # Just a number, assume sq ft
                result['area'] = f"{float(parts[0])} sqft"
        except ValueError:
            result['area'] = None
    
    # Parse property type from Key Details section
    for row in soup.select(".keyDetails-row"):
        value_type = row.select_one(".valueType")
        if value_type and "Property Type" in value_type.get_text():
            value = row.select_one(".valueText")
            if value:
                result['propertyType'] = value.get_text(strip=True)
                break
    
    # Parse year built from Key Details section
    for row in soup.select(".keyDetails-row"):
        value_type = row.select_one(".valueType")
        if value_type and "Year Built" in value_type.get_text():
            value = row.select_one(".valueText")
            if value:
                try:
                    result['yearBuilt'] = int(value.get_text(strip=True))
                except ValueError:
                    result['yearBuilt'] = None
                break
    
    # Parse lot size from Key Details section
    for row in soup.select(".keyDetails-row"):
        value_type = row.select_one(".valueType")
        if value_type and "Lot Size" in value_type.get_text():
            value = row.select_one(".valueText")
            if value:
                lot_size = value.get_text(strip=True)
                if lot_size and lot_size != "—":
                    # Clean the lot size text and preserve unit
                    lot_size_clean = lot_size.replace(",", "").strip()
                    try:
                        # Extract number and unit
                        parts = lot_size_clean.split(" ")
                        if len(parts) >= 2:
                            number = float(parts[0])
                            unit = "".join(parts[1:])
                            result['lotArea'] = f"{number} {unit}"
                        else:
                            # Just a number, assume sq ft
                            result['lotArea'] = f"{float(parts[0])} sq ft"
                    except ValueError:
                        result['lotArea'] = None
                break
    
    # Fallback: Try to find lot size in "Public facts" section
    if not result['lotArea']:
        for entry in soup.find_all("li", class_="entryItem"):
            label = entry.find("span", class_="entryItemContent")
            if label and "Lot Size:" in label.get_text():
                spans = entry.find_all("span")
                if len(spans) > 1:
                    lot_size = spans[1].get_text(strip=True)
                    if lot_size and lot_size != "—":
                        # Clean the lot size text and preserve unit
                        lot_size_clean = lot_size.replace(",", "").strip()
                        try:
                            # Extract number and unit
                            parts = lot_size_clean.split(" ")
                            if len(parts) >= 2:
                                number = float(parts[0])
                                unit = " ".join(parts[1:])
                                result['lotArea'] = f"{number} {unit}"
                            else:
                                # Just a number, assume sq ft
                                result['lotArea'] = f"{float(parts[0])} sq ft"
                        except ValueError:
                            result['lotArea'] = None
                break
    
    # Parse property status
    result['status'] = _parse_property_status(soup)

    # Parse price
    result['price'] = _parse_property_price(soup)

    # Parse "Ready to Build" tag
    result["readyToBuildTag"] = False
    if result['yearBuilt'] == None:
        result["readyToBuildTag"] = _parse_property_ready_to_build_tag(soup)

    # Parse property history using the same BeautifulSoup object
    property_history = parse_property_history(soup)
    result.update(property_history)
    
    return result

# Example url: https://www.redfin.com/WA/Redmond/Redmond/Plan-2C/home/188825778
def _parse_property_ready_to_build_tag(beautiful_soup: BeautifulSoup) -> bool:
    script_tags = beautiful_soup.find_all("script")
    keyword = "Ready to Build".lower()
    for script in script_tags:
        if script.string and keyword in script.string.lower():
            return True
    return False

def _parse_property_price(beautiful_soup: BeautifulSoup) -> Optional[float]:
    """
    Parse property price from HTML content.

    Args:
        beautiful_soup: BeautifulSoup object of the property page

    Returns:
        Property price as a float, or None if not found
    """
    price_tag = parse_meta_tag(beautiful_soup, "twitter:text:price")
    if price_tag:
        sanitized_price_str = price_tag.replace("$", "").replace(",", "")
        try:
            return float(sanitized_price_str)
        except ValueError:
            return None
    return None

def _parse_property_status(beautiful_soup: BeautifulSoup) -> Optional[str]:
    """
    Parse property status from HTML content.

    Args:
        beautiful_soup: BeautifulSoup object of the property page

    Returns:
        Property status normalized to "Active", "Pending", "Sold", or None if not found
    """
    def normalize_status(raw_status: str) -> Optional[str]:
        """
        Normalize raw status text to standard values.

        Args:
            raw_status: Raw status text from HTML
    
        Returns:
            Normalized status: "Active", "Pending", "Sold", or None
        """
        status_lower = raw_status.lower().strip()
        
        # Map various status texts to our three standard values
        if any(keyword in status_lower for keyword in ['for sale', 'active', 'listed', 'on market']):
            return 'Active'
        elif any(keyword in status_lower for keyword in ['pending', 'under contract', 'contingent']):
            return 'Pending'
        elif any(keyword in status_lower for keyword in ['sold', 'closed', 'sale closed']):
            return 'Sold'
        
        return None
    
    # Look for status in ListingStatusBannerSection
    status_banner = beautiful_soup.select_one('.ListingStatusBannerSection')
    if status_banner:
        status_text = status_banner.get_text(strip=True)
        if status_text:
            logger.info("Found status banner")
            normalized = normalize_status(status_text)
            if normalized:
                return normalized

    # Fallback: Look for status in the property history (most recent event)
    history_events = _parse_history_from_javascript(beautiful_soup)
    if not history_events:
        history_events = _parse_history_from_html(beautiful_soup)
    
    logger.warning("Fallback: checking property history to get status")
    if history_events:
        # Check the most recent event description for status clues
        latest_event = history_events[0]  # Assuming events are ordered by date
        description = latest_event.get('description', '').lower()
        
        if 'sold' in description:
            logger.info("Method 6 working - found SOLD in history")
            return 'Sold'
        elif any(keyword in description for keyword in ['listed', 'for sale']):
            logger.info("Method 6 working - found Active status in history")
            return 'Active'
        elif 'pending' in description:
            logger.info("Method 6 working - found Pending status in history")
            return 'Pending'
    
    return None

def parse_property_history(beautiful_soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Parse property history from HTML content.
    
    Args:
        beautiful_soup: BeautifulSoup object of the property page
    
    Returns:
        Dictionary containing property history information
    """
    # First, try to extract from JavaScript data (more complete)
    history_events_from_js = _parse_history_from_javascript(beautiful_soup)
    if history_events_from_js:
        logger.info(f"Successfully parsed {len(history_events_from_js)} events from JavaScript")
        return {
            'history': history_events_from_js,
            'historyCount': len(history_events_from_js)
        }
    
    # Fallback: Parse from HTML structure (less complete)
    logger.warning("No JavaScript data found, falling back to HTML parsing")
    history_events_from_html = _parse_history_from_html(beautiful_soup)
    
    return {
        'history': history_events_from_html,
        'historyCount': len(history_events_from_html)
    }

def _parse_history_from_html(beautiful_soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Parse property history from HTML structure.
    
    Args:
        beautiful_soup: BeautifulSoup object of the property page
    
    Returns:
        List of history events, or empty list if parsing fails
    """
    history_events = []
    
    for row in beautiful_soup.select(".PropertyHistoryEventRow"):
        event: Dict[str, Any] = {}
        
        # Parse date
        date_elem = row.select_one(".col-4 p")
        if date_elem:
            event['date'] = date_elem.get_text(strip=True)
        
        # Parse description/event type
        desc_elem = row.select_one(".description-col .col-4 div")
        if desc_elem:
            event['description'] = desc_elem.get_text(strip=True)
        
        # Parse price
        price_elem = row.select_one(".price-col.number")
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            if price_text and price_text != "—":
                # Clean price text (remove $ and commas)
                price_clean = price_text.replace("$", "").replace(",", "")
                try:
                    event['price'] = float(price_clean)
                except ValueError:
                    event['price'] = None
            else:
                event['price'] = None
        
        # Parse MLS number from subtext
        subtext_elem = row.select_one(".description-col p.subtext")
        if subtext_elem:
            subtext = subtext_elem.get_text(strip=True)
            if "MLS" in subtext:
                # Extract MLS number
                mls_match = re.search(r'#(\d+)', subtext)
                if mls_match:
                    event['mlsNumber'] = mls_match.group(1)
        
        # Only add event if we have at least a date
        if event.get('date'):
            history_events.append(event)
    
    return history_events

def _parse_history_from_javascript(beautiful_soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Parse property history from JavaScript data.
    
    Args:
        beautiful_soup: BeautifulSoup object of the property page
    
    Returns:
        List of history events, or empty list if parsing fails
    """
    history_events = []
    
    script_text = find_property_history_object(beautiful_soup)
    if not script_text:
        return []
    
    try:
        historyObject = json.loads(script_text)
        raw_events = historyObject.get("events", [])
        
        for raw_event in raw_events:
            event: Dict[str, Any] = {}
            
            # Extract date - prefer eventDateString if available, otherwise convert eventDate
            if 'eventDateString' in raw_event:
                event['date'] = raw_event['eventDateString']
            elif 'eventDate' in raw_event:
                # Convert timestamp to readable date
                timestamp = raw_event['eventDate'] / 1000  # Convert from milliseconds
                event['date'] = datetime.fromtimestamp(timestamp).strftime('%b %d, %Y')
            
            # Extract description
            if 'eventDescription' in raw_event:
                event['description'] = raw_event['eventDescription']
            
            # Extract price
            if 'price' in raw_event:
                event['price'] = raw_event['price']
            
            # Extract MLS number from sourceId
            if 'sourceId' in raw_event:
                event['mlsNumber'] = raw_event['sourceId']
            
            # Extract source
            if 'source' in raw_event:
                event['source'] = raw_event['source']
            
            # Only add event if we have at least a date
            if event.get('date'):
                history_events.append(event)
        
        logger.info(f"Successfully parsed {len(history_events)} events from JavaScript data")
        return history_events
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from JavaScript data: {e}", stack_info=True)
        return []
    except Exception as e:
        logger.error(f"Error parsing JavaScript history data: {e}", stack_info=True)
        return []
    
def find_property_history_object(beautiful_soup: BeautifulSoup) -> str:
    script_tags = beautiful_soup.find_all("script")
    script_text = ""
    for script in script_tags:
        if script.string and "propertyHistoryInfo" in script.string:
            script_text = script.string
            logger.info("Found script with propertyHistoryInfo")
            break
    
    # Find "propertyHistoryInfo" object
    start_index = script_text.find("propertyHistoryInfo")
    if start_index != -1:
        start_curly_braces_index = script_text.find("{", start_index)
        end_curly_braces_index = start_curly_braces_index + 1

        countOfStartCurlyBraces = 1
        while end_curly_braces_index < len(script_text):
            if script_text[end_curly_braces_index] == "{":
                countOfStartCurlyBraces += 1
            elif script_text[end_curly_braces_index] == "}":
                countOfStartCurlyBraces -= 1
            if countOfStartCurlyBraces == 0:
                break
            end_curly_braces_index += 1
        return script_text[start_curly_braces_index:end_curly_braces_index + 1].replace("\\\"", "\"")
    
    logger.info("No target Javascript data found")
    return ""

def parse_property_page(url: str, html_content: str, spider_name: str = "redfin_spider") -> Dict[str, Any]:
    """
    Parse a complete Redfin property page.
    
    Args:
        url: Property page URL
        html_content: HTML content of the property page
        spider_name: Name of the spider (for metadata)
    
    Returns:
        Dictionary containing all extracted property information
    """
    # Extract basic metadata
    redfin_id = extract_redfin_id(url)
    
    # Parse property details from HTML (includes history)
    property_details = parse_property_details(html_content)
    
    # Combine all data
    result = {
        'url': url,
        'redfinId': redfin_id,
        'scrapedAt': datetime.now().isoformat(),
        'spiderName': spider_name,
        **property_details
    }
    
    return result 