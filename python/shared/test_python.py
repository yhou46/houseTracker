import datetime as DatetimeLib
import os
from enum import Enum
import requests
from bs4 import BeautifulSoup
import usaddress # type: ignore[import-untyped]

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def parse_soldon_date(date_str: str) -> datetime:
    """
    Parse a string in the format 'soldon aug 5, 2025' and return a datetime object in Pacific Time.

    Args:
        date_str: The input string in the format 'soldon <month> <day>, <year>'.

    Returns:
        A datetime object in Pacific Time.
    """
    # Remove the "soldon" prefix and strip any extra whitespace
    date_part = date_str.replace("soldon", "").strip()

    # Parse the date part into a naive datetime object
    naive_date = datetime.strptime(date_part, "%b %d, %Y")

    # Assign the Pacific Timezone
    pacific_time = naive_date.replace(tzinfo=ZoneInfo("America/Los_Angeles"))

    return pacific_time

if __name__ == "__main__":
    l = [2,3,4,5]
    for i in range(len(l)-1, -1, -1):
        print(i)