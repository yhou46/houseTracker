# Configuration file for Redfin Spider
from typing import List

# Zip codes to crawl
ZIP_CODES: List[str] = [
    '98052',  # Example zip code
    # Add more zip codes here as needed
    # '98053',
    # '98054',
    # '98055',
]

# URL format for Redfin zip code searches
REDFIN_ZIP_URL_FORMAT = "https://www.redfin.com/zipcode/{zip_code}" 