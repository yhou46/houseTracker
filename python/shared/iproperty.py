from enum import Enum
from datetime import datetime, timezone
from typing import List

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
    SingleFamily = "SingleFamily"
    SingleFamilyOnWater = "SingleFamilyOnWater"
    Townhome = "Townhome"
    Condo = "Condo"
    VacantLand = "VacantLand"
    MultiFamily = "MultiFamily"
    Manufactured = "Manufactured"
    Coops = "Coops"

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

class PropertyHistoryEventType(Enum):
    Listed = "Listed"
    ReListed = "ReListed"
    DeListed = "DeListed"
    Pending = "Pending"
    PriceChange = "PriceChange"
    Sold = "Sold"
    Contingent = "Contingent"
    ListedForRent = "ListedForRent"
    RentalRemoved = "RentalRemoved"
    Other = "Other"

class IPropertyHistoryEvent:
    def __init__(
            self,
            datetime: datetime,
            eventType: PropertyHistoryEventType,
            description: str,
            source: str | None = None,
            sourceId: str | None = None,
            price: float | None = None,
            ):
        self._datetime = datetime
        self._eventType = eventType
        self._description = description
        self._price = price
        self._source = source
        self._sourceId = sourceId
    
    @property
    def datetime(self) -> datetime:
        return self._datetime
    @property
    def eventType(self) -> PropertyHistoryEventType:
        return self._eventType
    @property
    def description(self) -> str:
        return self._description
    @property
    def price(self) -> float | None:
        return self._price

    def __str__(self):
        return f"Date: {self.datetime.strftime('%Y-%m-%d')}, Event: {self.eventType.value}, Description: {self.description}, Price: {self.price if self.price is not None else 'N/A'}"

    def __eq__(self, other):
        if not isinstance(other, IPropertyHistoryEvent):
            return NotImplemented
        return (self._datetime == other._datetime and
                self._eventType == other._eventType and
                self._description == other._description and
                self._price == other._price and
                self._source == other._source and
                self._sourceId == other._sourceId)

# All prices are in USD
class IPropertyHistory:
    def __init__(
            self,
            property_id: str,
            address: IPropertyAddress,
            history: List[IPropertyHistoryEvent] | None = None,
            lastUpdated: datetime | None = None
            ):
        self._history = history if history is not None else []
        self._property_id = property_id
        self._address = address
        self._lastUpdated = lastUpdated if lastUpdated is not None else datetime.now(timezone.utc)

    def addEvent(self, event: IPropertyHistoryEvent):
        self._history.append(event)
        self._history.sort(key = lambda event: event._datetime) # Sort by date
        self._lastUpdated = datetime.now(timezone.utc)

    @property
    def id(self) -> str:
        return self._property_id

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
        return f"Property ID: {self._property_id},\nAddress: {self._address.get_address_hash()},\nHistory:\n{historyStr if historyStr else 'No history available'},\nlastUpdated: {self.lastUpdated.strftime('%Y-%m-%d %H:%M:%S')}"

# TODO:
# How to deal with vacant land? It has many properties as none, like numberOfBedrooms, numberOfBathrooms, yearBuilt, etc.
# Ready to built home doesn't have year built
class IPropertyBasic:
    def __init__(
        self,
        id: str,
        address: IPropertyAddress,
        area: PropertyArea,
        propertyType: PropertyType,
        lotArea: PropertyArea | None,
        numberOfBedrooms: float,
        numberOfBathrooms: float,
        yearBuilt: int | None,
    ):
        self.id: str = id
        self.address = address
        self.area = area
        self.propertyType = propertyType
        self.lotArea = lotArea
        self.numberOfBedrooms = numberOfBedrooms
        self.numberOfBathrooms = numberOfBathrooms
        self.yearBuilt = yearBuilt
    
    def __str__(self):
        return f"Basic property information: \naddress: {self.address},\nproperty type: {self.propertyType.value}, \narea: {self.area}, \nlot area: {self.lotArea}, \nnumberOfBedrooms: {self.numberOfBedrooms}, \nnumberOfBathrooms: {self.numberOfBathrooms}, \nyearBuilt: {self.yearBuilt}"

class PropertyStatus(Enum):
    Active = "Active"
    Pending = "Pending"
    Sold = "Sold"

class IPropertyDataSource:
    def __init__(self, sourceId: str, sourceUrl: str, sourceName: str):
        self.sourceId: str = sourceId
        self.sourceUrl: str = sourceUrl
        self.sourceName: str = sourceName
    
    def __str__(self):
        return f"Name: {self.sourceName}, Source ID: {self.sourceId}, URL: {self.sourceUrl}"

class IProperty(IPropertyBasic):
    def __init__(
        self,
        id: str,
        address: IPropertyAddress,
        area: PropertyArea,
        propertyType: PropertyType,
        lotArea: PropertyArea | None,
        numberOfBedrooms: float,
        numberOfBathrooms: float,
        yearBuilt: int | None,
        status: PropertyStatus,
        price: float | None,
        history: IPropertyHistory,
        lastUpdated: datetime,
        dataSource: List[IPropertyDataSource] = [],
    ):
        super().__init__(id, address, area, propertyType, lotArea, numberOfBedrooms, numberOfBathrooms, yearBuilt)
        self.status = status
        self.price = price
        self.history = history
        self._lastUpdated = lastUpdated if lastUpdated is not None else datetime.now(timezone.utc)
        self.dataSource = dataSource

    @property
    def lastUpdated(self) -> datetime:
        return self._lastUpdated

    def __str__(self):
        return (
            super().__str__() +
            f",\nstate: {self.status.value},\nprice: {self.price if self.price is not None else 'N/A'},\ndataSource:\n{",\n".join(str(source)for source in self.dataSource)},\nlastUpdated: {self.lastUpdated.strftime('%Y-%m-%d %H:%M:%S')}\nhistory:\n{self.history}\n"
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
        IPropertyAddress(address),
        PropertyArea(1700, AreaUnit.SquareFeet),
        PropertyType.SingleFamily,
        area,
        3,
        2.5,
        1899,
    )

    print(property1)