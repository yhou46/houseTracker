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
        'address': str | None,
        'area': str | None,
        'propertyType': str | None,
        'lotArea': str | None,
        'numberOfBedroom': float | None,
        'numberOfBathroom': float | None,
        'yearBuilt': int | None,
    }
    
    # Parse address from title
    title = soup.title
    if title and title.string:
        parts = title.string.split("|")
        if len(parts) >= 2:
            result['address'] = parts[0].strip()
    
    # Parse metadata from meta tags
    def parse_meta_tag(name: str) -> Optional[str]:
        meta_tag = soup.find("meta", attrs={"name": name})
        if meta_tag and hasattr(meta_tag, 'attrs'):
            attrs = meta_tag.attrs
            if isinstance(attrs, dict) and "content" in attrs:
                return str(attrs["content"])
        return None
    
    # Parse bedrooms and bathrooms
    beds = parse_meta_tag("twitter:text:beds")
    if beds:
        try:
            result['numberOfBedroom'] = float(beds)
        except ValueError:
            result['numberOfBedroom'] = None
    
    baths = parse_meta_tag("twitter:text:baths")
    if baths:
        try:
            result['numberOfBathroom'] = float(baths)
        except ValueError:
            result['numberOfBathroom'] = None
    
    # Parse square footage
    sqft = parse_meta_tag("twitter:text:sqft")
    if sqft:
        sqft_clean = sqft.replace(",", "")
        try:
            # Extract number and unit
            parts = sqft_clean.split(" ")
            if len(parts) >= 2:
                number = float(parts[0])
                unit = " ".join(parts[1:])
                result['area'] = f"{number} {unit}"
            else:
                # Just a number, assume sq ft
                result['area'] = f"{float(parts[0])} sq ft"
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
                            unit = " ".join(parts[1:])
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
    
    # Parse property history using the same BeautifulSoup object
    property_history = parse_property_history(soup)
    result.update(property_history)
    
    return result

def parse_property_history(beautiful_soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Parse property history from HTML content.
    
    Args:
        beautiful_soup: BeautifulSoup object of the property page
    
    Returns:
        Dictionary containing property history information
    """
    history_events = []
    
    # First, try to extract from JavaScript data (more complete)
    js_events = _parse_history_from_javascript(beautiful_soup)
    if js_events:
        print(f"Successfully parsed {len(js_events)} events from JavaScript")
        return {
            'history': js_events,
            'historyCount': len(js_events)
        }
    
    # Fallback: Parse from HTML structure (less complete)
    print("No JavaScript data found, falling back to HTML parsing")
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
    
    return {
        'history': history_events,
        'historyCount': len(history_events)
    }

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
    historyObject = json.loads(script_text)
    history_events = historyObject["events"]
    
    return history_events
    
def find_property_history_object(beautiful_soup: BeautifulSoup) -> str:
    script_tags = beautiful_soup.find_all("script")
    script_text = ""
    for script in script_tags:
        if script.string and "propertyHistoryInfo" in script.string:
            script_text = script.string
            print("Found script with propertyHistoryInfo")
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
    
    print("No target Javascript data found")
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