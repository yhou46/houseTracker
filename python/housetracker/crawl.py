import datetime as DatetimeLib
import os
from enum import Enum
import requests
from bs4 import BeautifulSoup
import uuid
import usaddress # type: ignore

# Make python aware of the module, no needed if edited PYTHONPATH
# import sys, os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from housetracker.iproperty import IPropertyBasic, IPropertyPriceList, PropertyArea, PropertyType, AreaUnit

def getRedfinResponse(url: str) -> str:
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

def parsePropertyType(soup: BeautifulSoup) -> PropertyType:
    propertyTypeStr: str = ""
    # Find the "Property Type" value in the Key Details section
    for row in soup.select(".keyDetails-row"):
        value_type = row.select_one(".valueType")
        if value_type and "Property Type" in value_type.get_text():
            value = row.select_one(".valueText")
            if value:
                propertyTypeStr = value.get_text(strip=True) 
                print(value.get_text(strip=True))
                break
    if propertyTypeStr == "":
        raise RuntimeError("Failed to parse property type")
    
    if propertyTypeStr == "Single-family":
        return PropertyType.SingleFamily
    elif propertyTypeStr == "Townhome":
        return PropertyType.Townhome
    elif propertyTypeStr == "Condo":
        return PropertyType.Condo
    else:
        raise RuntimeError(f"Unsupported property type: {propertyTypeStr}")

def parseLotSize(soup: BeautifulSoup) -> PropertyArea | None:
    areaSize: str = ""
    # Look for all rows in the Key Details section
    for row in soup.select(".keyDetails-row"):
        value_type = row.select_one(".valueType")
        if value_type and "Lot Size" in value_type.get_text():
            value = row.select_one(".valueText")
            if value:
                areaSize = value.get_text(strip=True)

    if areaSize == "":
        # Fallback: Try to find in the "Public facts" section
        for entry in soup.find_all("li", class_="entryItem"):
            label = entry.find("span", class_="entryItemContent")
            if label and "Lot Size:" in label.get_text():
                # The value is in the next <span>
                spans = entry.find_all("span")
                if len(spans) > 1:
                    areaSize = spans[1].get_text(strip=True)
    
    if areaSize != "" and areaSize != "—":
        print(f"Lot Size: {areaSize}")
        # Parse area size, e.g. "0.34 acres", "5,000 sq ft"
        # Get rid of commas
        areaSize = areaSize.replace(",", "")
        elements = areaSize.split(" ")
        areaNumber: float = float(elements[0])
        areaUnit: AreaUnit = AreaUnit.SquareFeet
        if elements[1].lower() in ["acres", "acre"]:
            areaUnit = AreaUnit.Acres
        return PropertyArea(areaNumber, areaUnit)
    
    # Lot size not found
    return None

# Parse a Redfin page and return some metadata
def parseHtml (content: str) -> tuple[IPropertyBasic, IPropertyPriceList]:
    soup = BeautifulSoup(content, "html.parser")

    # Parse title to get address
    title = soup.title
    address: str = ""
    if title != None and title.string != None:
        assert title.string is not None
        parts = title.string.split("|")
        if len(parts) >= 2:
            address = parts[0].strip()
            # MLS number format: MLS# 2301123
            # mlsNumber = parts[1].strip().split("#")[1].strip()
            print(f"Address: {address}")
        else:
            raise ValueError("Input string does not contain enough parts separated by '|'")
    else:
        raise RuntimeError(f"Failed to parse Html title: {title}")

    def parseMetaTag(soupObject: BeautifulSoup, name: str) -> str:
        meta_tag = soupObject.find("meta", attrs={"name": name})
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
    areaInSqft = parseMetaTag(soup, "twitter:text:sqft").replace(",", "")

    propertyType = parsePropertyType(soup)
    lotSize = parseLotSize(soup)

    print(f"Street address: {streetAddress}, City: {city}, State: {stateCode}, Zip: {zipCode}, Price: {price}, Beds: {numberOfBedrooms}, Baths: {numberOfBathrooms}, Sqft: {areaInSqft}, PropertyType: {propertyType}")

    propertyId = uuid.uuid4()
    propertyBasic = IPropertyBasic(
        id = str(propertyId),
        address = address,
        area = PropertyArea(float(areaInSqft)),
        numberOfBedrooms = float(numberOfBedrooms),
        numberOfBathrooms = float(numberOfBathrooms),
        propertyType = propertyType,
        lotArea = lotSize,
    )

    currentTime = DatetimeLib.datetime.now()
    priceList = IPropertyPriceList(propertyBasic.id, propertyBasic.address, [(currentTime, float(price.replace("$", "").replace(",", "")))])

    return (propertyBasic, priceList)

def endToEndTest(url: str):
    result = getRedfinResponse(url)
    soup = BeautifulSoup(result, "html.parser")

    timeStr = DatetimeLib.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    fileName = "redfinResponse_" + timeStr + ".html"
    filePath = os.path.dirname(os.path.abspath(__file__)) + "/../tmp"
    print(filePath)
    with open(f"{filePath}/{fileName}", "w") as file:
        file.write(soup.prettify())
    property, priceList = parseHtml(result)

    print(property)
    print(priceList)
    
def testWithHtmlFile(filePath: str):
    with open(filePath, 'r') as file:
        content = file.read()
    property, priceList = parseHtml(content)
    print(property)
    print(priceList)

if __name__ == "__main__":
    # test()

    # Test parse
    # content = readFileToString("./redfinResponse.txt")
    # parseHtml(content)

    # Get page
    # url = 'https://www.redfin.com/WA/Kirkland/11095-Champagne-Point-Rd-NE-98034/home/280166'
    # # url = "https://www.redfin.com/WA/Kirkland/1838-Market-St-98033/home/11902466"
    # result = getRedfinResponse(url)
    # soup = BeautifulSoup(result, "html.parser")

    # timeStr = DatetimeLib.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    # fileName = "redfinResponse_" + timeStr + ".html"
    # filePath = os.path.dirname(os.path.abspath(__file__)) + "/../tmp"
    # print(filePath)
    # with open(f"{filePath}/{fileName}", "w") as file:
    #     file.write(soup.prettify())
    # property, priceList = parseHtml(result)

    # print(property)
    # print(priceList)
    
    url = 'https://www.redfin.com/WA/Kirkland/327-2nd-Ave-S-98033/home/196065254'
    endToEndTest(url)

    # testWithHtmlFile("./tmp/redfinResponse_2025-06-12T22-22-57.html")