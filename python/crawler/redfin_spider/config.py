# Configuration file for Redfin Spider
from typing import List, Dict

# Zip codes to crawl
ZIP_CODES: List[str] = [
    "98109",
    # '98052',
    # '98034',
    # '98054',
    # '98055',
]

# URL format for Redfin zip code searches
REDFIN_ZIP_URL_FORMAT = "https://www.redfin.com/zipcode/{zip_code}"

CITY_URL_MAP: Dict[str, str] = {
    # "WA/Redmond": "https://www.redfin.com/city/14913/WA/Redmond",
    # "WA/Kirkland": "https://www.redfin.com/city/9148/WA/Kirkland",
    # "WA/Kenmore": "https://www.redfin.com/city/8944/WA/Kenmore",
    # "WA/Bothel": "https://www.redfin.com/city/29439/WA/Bothell",
    # "WA/Lynnwood": "https://www.redfin.com/city/10421/WA/Lynnwood"
}