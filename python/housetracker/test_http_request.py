import datetime as DatetimeLib
import os
from enum import Enum
import requests
from bs4 import BeautifulSoup
import usaddress # type: ignore



#-------------------------

def getRedfinResponse(url: str):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Page fetched successfully!")

    else:
        print(f"Failed to fetch page: {response.status_code}")
        raise RuntimeError(f"Failed to get response from url: {url}")

    return response.text

# Parse a Redfin page and return some metadata
def parseHtml (content: str):
    soup = BeautifulSoup(content, "html.parser")

    # Parse title to get address
    title = soup.title
    if title != None and title.string != None:
        assert title.string is not None
        parts = title.string.split("|")
        if len(parts) >= 2:
            address = parts[0].strip()
            # MLS number format: MLS# 2301123
            mlsNumber = parts[1].strip().split("#")[1].strip()
            print(f"Address: {address}, MLS: {mlsNumber}")
        else:
            raise ValueError("Input string does not contain enough parts separated by '|'")
    else:
        raise RuntimeError(f"Failed to parse Html title: {title}")

    def parseMetaTag(soupObject: BeautifulSoup, name: str) -> str:
        meta_tag = soup.find("meta", attrs={"name": name})
        if meta_tag and "content" in meta_tag.attrs: # type: ignore
            value: str = meta_tag["content"] # type: ignore
            return value
        else:
            raise RuntimeError(f"Failed to parse meta tag with name: {name}")

    # Parse metadata of a property
    streetAddress = parseMetaTag(soup, "twitter:text:street_address")
    city = parseMetaTag(soup, "twitter:text:city")
    stateCode = parseMetaTag(soup, "twitter:text:state_code")
    zipCode = parseMetaTag(soup, "twitter:text:zip")
    price = parseMetaTag(soup, "twitter:text:price")
    numberOfBedrooms = parseMetaTag(soup, "twitter:text:beds")
    numberOfBathrooms = parseMetaTag(soup, "twitter:text:baths")
    areaInSqft = parseMetaTag(soup, "twitter:text:sqft")

    print(f"Street address: {streetAddress}, City: {city}, State: {stateCode}, Zip: {zipCode}, Price: {price}, Beds: {numberOfBedrooms}, Baths: {numberOfBathrooms}, Sqft: {areaInSqft}")





def test():
    address = "Apt 116, 6910 Old Redmond Rd, Redmond, WA, 98052"
    addressObj = IPropertyAddress(address)

    property1 = IPropertyBasic(
        "abc",
        address,
        IPropertyArea(1700, "sqft"),
        PropertyType.SingleFamily,
        None,
        3,
        2,
    )
    print(addressObj)
    print(property1)

def readFileToString(filePath: str) -> str:
    with open(filePath, 'r') as file:
        content = file.read()
    return content

if __name__ == "__main__":
    # test()

    # Test parse
    # content = readFileToString("./redfinResponse.txt")
    # parseHtml(content)

    # Get page
    url = 'https://www.redfin.com/WA/Kenmore/7210-NE-158th-St-98028/home/275163'
    result = getRedfinResponse(url)
    soup = BeautifulSoup(result, "html.parser")

    timeStr = DatetimeLib.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    fileName = "redfinResponse_" + timeStr + ".txt"
    filePath = os.path.dirname(os.path.abspath(__file__)) + "/../tmp"
    print(filePath)
    with open(f"{filePath}/{fileName}", "w") as file:
        file.write(soup.prettify())
    parseHtml(result)

    # parse page
    # currentPath = os.path.dirname(__file__)
    # print(currentPath)
    # with open(f"{currentPath}/redfinResponse.txt", "r") as file:
    #     content = file.read()
    # parseHtml(content)