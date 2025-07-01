import datetime as DatetimeLib
import os
from enum import Enum
import requests
from bs4 import BeautifulSoup
import usaddress # type: ignore

if __name__ == "__main__":
    timeNow = DatetimeLib.datetime.now(DatetimeLib.timezone.utc)
    print(f"Current time: {timeNow.strftime('%Y-%m-%d %H:%M:%S')}")
    print(timeNow)
    print(timeNow.tzinfo)
    print(timeNow.astimezone())