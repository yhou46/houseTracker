import datetime as DatetimeLib
from enum import Enum
import requests
from bs4 import BeautifulSoup
import usaddress # type: ignore

## Define some interfaces
class IPropertyPriceList:
    def __init__(self, priceList: list[tuple[float, DatetimeLib.datetime]]):
        self.priceList: list[tuple[float, DatetimeLib.datetime]] = priceList

class IPropertyArea:
    def __init__(self, area: float, unit: str):
        self.area: float = area
        self.unit: str = unit

class AddressType(Enum):
    StreetAddress = "Street Address"
    Intersection = "Intersection"
    POBox = "PO Box"
    Ambiguous = "Ambiguous"

class PropertyType(Enum):
    SingleFamily = "Single-family"
    Townhome = "Townhome"
    Condo = "Condo"


# TODO: Use USPS address format API?
class IPropertyAddress:
    def __init__(self, address: str):
        self.addressLine: str = address
        parsedAddress = usaddress.tag(address)
        addressType: str = parsedAddress[1]
        if (addressType != AddressType.StreetAddress.value):
            raise ValueError(f"Invalid address type: {addressType} for address: {address}")
        addressPropertyBag: dict = parsedAddress[0]
        self.streetName: str = addressPropertyBag["AddressNumber"]
        self.state: str = addressPropertyBag["StateName"]
        self.city: str = addressPropertyBag["PlaceName"]
        self.zipCode: str = addressPropertyBag["ZipCode"]

    def __str__(self):
        return f"Full address: {self.addressLine}, Street: {self.streetName}, State: {self.state}, ZipCode: {self.zipCode}"
        

class IProperty:
    id: str

    # Basic information
    address: IPropertyAddress
    priceHistory: IPropertyPriceList
    area: IPropertyArea
    lotArea: IPropertyArea | None
    propertyType: PropertyType

    # Properties from gov website
    county: str
    parcelNumber: str
    numberOfBedrooms: float
    numberOfBathrooms: float
    taxHistory: list[float]

class IPropertyBasic:
    def __init__(
        self,
        id: str,
        address: str,
        area: IPropertyArea,
        propertyType: PropertyType,
        lotArea: IPropertyArea | None,
        numberOfBedrooms: float,
        numberOfBathrooms: float,
    ):
        self.id: str = id
        self.address: str = address
        self.area = area
        self.propertyType = propertyType
        self.lotArea = lotArea
        self.numberOfBedrooms = numberOfBedrooms
        self.numberOfBathrooms = numberOfBathrooms
    
    def __str__(self):
        return f"Property address: {self.address}, type: {self.propertyType.value}"

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

    with open("./redfinResponse2.txt", "w") as file:
        file.write(soup.prettify())
    parseHtml(result)

    # parse page
    # currentPath = os.path.dirname(__file__)
    # print(currentPath)
    # with open(f"{currentPath}/redfinResponse.txt", "r") as file:
    #     content = file.read()
    # parseHtml(content)