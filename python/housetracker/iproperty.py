from enum import Enum
import usaddress # type: ignore
import uuid

## Define some interfaces
# class IPropertyPriceList:
#     def __init__(self, priceList: list[tuple[float, DatetimeLib.datetime]]):
#         self.priceList: list[tuple[float, DatetimeLib.datetime]] = priceList

class AreaUnit(Enum):
    SquareFeet = "SquareFeet"
    SquareMeter = "SquareMeter"

class IPropertyArea:
    def __init__(self, area: float, unit: AreaUnit = AreaUnit.SquareFeet):
        self.area: float = area
        self.unit: AreaUnit = unit

class AddressType(Enum):
    StreetAddress = "Street Address"
    Intersection = "Intersection"
    POBox = "PO Box"
    Ambiguous = "Ambiguous"

class PropertyType(Enum):
    SingleFamily = "Single-family"
    Townhome = "Townhome"
    Condo = "Condo"

# Concat different tags into a single street address
# usaddress doc: https://parserator.datamade.us/api-docs/
def concatenateStreetAddress(addressPropertyBag: dict) -> str:
    addressTags = set({
        "AddressNumber",
        "AddressNumberPrefix",
        "AddressNumberSuffix",
        "BuildingName",
        "CornerOf",
        "IntersectionSeparator",
        "LandmarkName",
        "NotAddress",
        #"OccupancyIdentifier",
        #"OccupancyType",
        "StreetName",
        "StreetNamePostDirectional",
        "StreetNamePostModifier",
        "StreetNamePostType",
        "StreetNamePreDirectional",
        "StreetNamePreModifier",
        "StreetNamePreType",
        "SubaddressIdentifier",
        "SubaddressType",
    })
    streetAddress: str = ""
    for key, value in addressPropertyBag.items():
        if key in addressTags:
            streetAddress += (" " if len(streetAddress) > 0 else "") + value
    return streetAddress

# TODO: Use USPS address format API?
class IPropertyAddress:
    def __init__(self, address: str):
        self.fullAddress: str = address
        parsedAddress = usaddress.tag(address)
        addressType: str = parsedAddress[1]
        if (addressType != AddressType.StreetAddress.value):
            raise ValueError(f"Invalid address type: {addressType} for address: {address}")
        addressPropertyBag: dict = parsedAddress[0]
        self.streetName: str = addressPropertyBag["AddressNumber"] + " " + addressPropertyBag["StreetName"] + " " + addressPropertyBag["StreetNamePostType"]
        self.state: str = addressPropertyBag["StateName"]
        self.city: str = addressPropertyBag["PlaceName"]
        self.zipCode: str = addressPropertyBag["ZipCode"]

    def __str__(self):
        return f"Full address: {self.fullAddress}, Street: {self.streetName}, State: {self.state}, ZipCode: {self.zipCode}"
        

# class IProperty:
#     id: str

#     # Basic information
#     address: IPropertyAddress
#     area: IPropertyArea
#     lotArea: IPropertyArea | None
#     propertyType: PropertyType

#     # MLS
#     mlsNumber: str

#     # Properties from gov website
#     county: str
#     parcelNumber: str
#     numberOfBedrooms: float
#     numberOfBathrooms: float
#     taxHistory: list[float]

#     # Properties that subject to change
#     priceHistory: IPropertyPriceList

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

if __name__ == "__main__":
    # Test the IPropertyAddress class
    address = "1838 Market St,Kirkland, WA 98033"
    addressObj = IPropertyAddress(address)
    area = IPropertyArea(2879)
    print(addressObj)

    # Test the IPropertyBasic class
    propertyId = uuid.uuid4()
    property1 = IPropertyBasic(
        str(propertyId),
        address,
        IPropertyArea(1700, AreaUnit.SquareFeet),
        PropertyType.SingleFamily,
        area,
        3,
        2.5,
    )
    print(property1)