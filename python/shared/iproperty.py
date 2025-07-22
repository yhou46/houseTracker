from enum import Enum
from datetime import datetime, timezone

import uuid
import logging

from shared.iproperty_address import IPropertyAddress

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

class PropertyType(Enum):
    SingleFamily = "Single-family"
    Townhome = "Townhome"
    Condo = "Condo"

# Deprecated
def _extractStreetAddress(addressPropertyBag: dict) -> str:
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

class IPropertyHistoryEventType(Enum):
    Listed = "Listed"
    ReListed = "ReListed"
    DeListed = "DeListed"
    Pending = "Pending"
    PriceChange = "PriceChange"
    Sold = "Sold"
    Other = "Other"

class IPropertyHistoryEvent:
    def __init__(
            self,
            datetime: datetime,
            eventType: IPropertyHistoryEventType,
            description: str,
            price: float | None = None,
            ):
        self._datetime = datetime
        self._eventType = eventType
        self._description = description
        self._price = price

        if eventType == IPropertyHistoryEventType.PriceChange and price is None:
            raise ValueError("Price must be provided for PriceChange event type")
    
    @property
    def datetime(self) -> datetime:
        return self._datetime
    @property
    def eventType(self) -> IPropertyHistoryEventType:
        return self._eventType
    @property
    def description(self) -> str:
        return self._description
    @property
    def price(self) -> float | None:
        return self._price

    def __str__(self):
        return f"Date: {self.datetime.strftime('%Y-%m-%d')}, Event: {self.eventType.value}, Description: {self.description}, Price: {self.price if self.price is not None else 'N/A'}"

# All prices are in USD
class IPropertyHistory:
    def __init__(
            self,
            id: str,
            address: IPropertyAddress,
            history: list[IPropertyHistoryEvent] | None = None,
            lastUpdated: datetime | None = None
            ):
        self._history = history if history is not None else []
        self._id = id
        self._address = address
        self._lastUpdated = lastUpdated if lastUpdated is not None else datetime.now(timezone.utc)

    def addEvent(self, event: IPropertyHistoryEvent):
        self._history.append(event)
        self._history.sort(key = lambda event: event._datetime) # Sort by date
        self._lastUpdated = datetime.now(timezone.utc)

    @property
    def id(self) -> str:
        return self._id

    @property
    def address(self) -> IPropertyAddress:
        return self._address

    @property
    def history(self) -> list[IPropertyHistoryEvent]:
        return self._history

    @property
    def lastUpdated(self) -> datetime:
        return self._lastUpdated

    def __str__(self):
        historyStr = "\n".join(str(event) for event in self._history)
        return f"Property ID: {self._id},\nAddress: {self._address.getAddressLine()},\nHistory:\n{historyStr if historyStr else 'No history available'},\nlastUpdated: {self.lastUpdated.strftime('%Y-%m-%d %H:%M:%S')}"

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
        return f"Basic property information: \naddress: {self.address},\nproperty type: {self.propertyType.value}, \narea: {self.area}, \nlot area: {self.lotArea}, \nnumberOfBedrooms: {self.numberOfBedrooms}, \nnumberOfBathrooms: {self.numberOfBathrooms}, \nyearBuilt: {self.yearBuilt}"

class IPropertyState(Enum):
    Active = "Active"
    Pending = "Pending"
    Sold = "Sold"

class IProperty(IPropertyBasic):
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
        state: IPropertyState,
        price: float | None,
        lastUpdated: datetime | None = None,
        redfinId: str | None = None
    ):
        super().__init__(id, address, area, propertyType, lotArea, numberOfBedrooms, numberOfBathrooms, yearBuilt)
        self.state = state
        self.price = price
        self._lastUpdated = lastUpdated if lastUpdated is not None else datetime.now(timezone.utc)
        self.redfinId = redfinId

    @property
    def lastUpdated(self) -> datetime:
        return self._lastUpdated

    def __str__(self):
        return (
            super().__str__() +
            f", \nstate: {self.state.value},\nprice: {self.price if self.price is not None else 'N/A'},\nredfinId: {self.redfinId},\nlastUpdated: {self.lastUpdated.strftime('%Y-%m-%d %H:%M:%S')}"
        )

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