from enum import Enum
from datetime import datetime

import usaddress # type: ignore
import uuid


class AreaUnit(Enum):
    SquareFeet = "SquareFeet"
    SquareMeter = "SquareMeter"
    Acres = "Acres"

class PropertyArea:
    def __init__(self, area: float, unit: AreaUnit = AreaUnit.SquareFeet):
        self.area: float = area
        self.unit: AreaUnit = unit

    def __eq__(self, value):
        if not isinstance(value, PropertyArea):
            return NotImplemented
        return self.area == value.area and self.unit == value.unit

    def __str__(self):
        return f"{self.area} {self.unit.value}"

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
def extractStreetAddress(addressPropertyBag: dict) -> str:
    addressTags = set({
        "AddressNumber",
        "AddressNumberPrefix",
        "AddressNumberSuffix",
        "BuildingName",
        "CornerOf",
        "IntersectionSeparator",
        "LandmarkName",
        "NotAddress",
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

def extractUnitInformation(addressPropertyBag: dict) -> str:
    addressTags = set({
        "OccupancyIdentifier",
        "OccupancyType",
    })
    unit: str = ""
    for key, value in addressPropertyBag.items():
        if key in addressTags:
            unit += (" " if len(unit) > 0 else "") + value
    return unit

# TODO: Use USPS address format API?
class IPropertyAddress:
    def __init__(self, address: str):
        parsedAddress = usaddress.tag(address)
        addressType: str = parsedAddress[1]
        if (addressType != AddressType.StreetAddress.value):
            raise ValueError(f"Invalid address type: {addressType} for address: {address}")
        addressPropertyBag: dict = parsedAddress[0]
        self.streetName: str = extractStreetAddress(addressPropertyBag)
        self.unit: str = extractUnitInformation(addressPropertyBag)
        self.state: str = addressPropertyBag["StateName"]
        self.city: str = addressPropertyBag["PlaceName"]
        self.zipCode: str = addressPropertyBag["ZipCode"]

        self.fullAddress: str = self.streetName + "," + ((self.unit + ",") if len(self.unit) > 0 else "") + self.city + "," + self.state + "," + self.zipCode

    def getAddressLine(self) -> str:
        return self.fullAddress

    # This is index related
    def __eq__(self, other):
        if not isinstance(other, IPropertyAddress):
            return NotImplemented
        return self.fullAddress == other.fullAddress

    def __str__(self):
        return f"Full address: {self.fullAddress}, Street: {self.streetName}, UnitNumber(if any): {self.unit}, State: {self.state}, ZipCode: {self.zipCode}"

class IPropertyHistoryEventType(Enum):
    Listed = "Listed"
    ReListed = "ReListed"
    DeListed = "DeListed"
    Pending = "Pending"
    PriceChange = "PriceChange"
    Sold = "Sold"
    Other = "Other"

class IPropertyHistoryEvent:
    def __init__(self, datetime: datetime, eventType: IPropertyHistoryEventType, description: str, price: float | None = None):
        self.datetime = datetime
        self.eventType = eventType
        self.description = description
        self.price = price

        if eventType == IPropertyHistoryEventType.PriceChange and price is None:
            raise ValueError("Price must be provided for PriceChange event type")
    
    def __str__(self):
        return f"Date: {self.datetime.strftime('%Y-%m-%d')}, Event: {self.eventType.value}, Description: {self.description}, Price: {self.price if self.price is not None else 'N/A'}"

# All prices are in USD
class IPropertyHistory:
    def __init__(self, id: str, address: IPropertyAddress, history: list[IPropertyHistoryEvent] | None = None):
        self.history = history if history is not None else []
        self.id = id
        self.address = address

    def addEvent(self, event: IPropertyHistoryEvent):
        self.history.append(event)
        self.history.sort(key = lambda event: event.datetime) # Sort by date
    
    def __str__(self):
        historyStr = "\n".join(str(event) for event in self.history)
        return f"Property ID: {self.id},\nAddress: {self.address.getAddressLine()},\nHistory:\n{historyStr if historyStr else 'No history available'}"

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
        area: PropertyArea,
        propertyType: PropertyType,
        lotArea: PropertyArea | None,
        numberOfBedrooms: float,
        numberOfBathrooms: float,
        yearBuilt: int,
    ):
        self.id: str = id
        self.address: IPropertyAddress = IPropertyAddress(address)
        self.area = area
        self.propertyType = propertyType
        self.lotArea = lotArea
        self.numberOfBedrooms = numberOfBedrooms
        self.numberOfBathrooms = numberOfBathrooms
        self.yearBuilt = yearBuilt
    
    def __str__(self):
        return f"Property information: \naddress: {self.address},\nproperty type: {self.propertyType.value}, \narea: {self.area}, \nlot area: {self.lotArea}, \nnumberOfBedrooms: {self.numberOfBedrooms}, \nnumberOfBathrooms: {self.numberOfBathrooms}, \nyearBuilt: {self.yearBuilt}"

if __name__ == "__main__":
    # Test the IPropertyAddress class
    address = "1838 Market St,Kirkland, WA 98033"
    addressObj = IPropertyAddress(address)
    area = PropertyArea(2879)
    print(addressObj)

    # Test the IPropertyBasic class
    propertyId = uuid.uuid4()
    property1 = IPropertyBasic(
        str(propertyId),
        address,
        PropertyArea(1700, AreaUnit.SquareFeet),
        PropertyType.SingleFamily,
        area,
        3,
        2.5,
        1899,
    )
    print(property1)