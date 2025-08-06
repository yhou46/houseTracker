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
def _extractStreetAddress(address_property_bag: dict) -> str:
    address_tags = set({
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
    street_address: str = ""
    for key, value in address_property_bag.items():
        if key in address_tags:
            street_address += (" " if len(street_address) > 0 else "") + value
    return street_address

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
            event_type: PropertyHistoryEventType,
            description: str,
            source: str | None = None,
            source_id: str | None = None,
            price: float | None = None,
            ):
        self._datetime = datetime
        self._event_type = event_type
        self._description = description
        self._price = price
        self._source = source
        self._source_id = source_id
        self._id = str(uuid.uuid4())  # Unique ID for the event
    
    @property
    def datetime(self) -> datetime:
        return self._datetime
    @property
    def event_type(self) -> PropertyHistoryEventType:
        return self._event_type
    @property
    def description(self) -> str:
        return self._description
    @property
    def price(self) -> float | None:
        return self._price
    @property
    def id(self) -> str:
        return self._id
    @property
    def source(self) -> str | None:
        return self._source
    @property
    def source_id(self) -> str | None:
        return self._source_id

    def __str__(self):
        return f"Date: {self.datetime.strftime('%Y-%m-%d')}, Event: {self.event_type.value}, Description: {self.description}, Price: {self.price if self.price is not None else 'N/A'}, Source: {self._source if self._source else 'N/A'}, Source ID: {self._source_id if self._source_id else 'N/A'}"

    def __eq__(self, other):
        if not isinstance(other, IPropertyHistoryEvent):
            return NotImplemented
        return (self._datetime == other._datetime and
                self._event_type == other._event_type and
                self._price == other._price and
                self._source == other._source and
                self._source_id == other._source_id)

# All prices are in USD
class IPropertyHistory:
    def __init__(
            self,
            property_id: str,
            address: IPropertyAddress,
            history: List[IPropertyHistoryEvent] | None = None,
            last_updated: datetime | None = None
            ):
        self._history = history if history is not None else []
        self._property_id = property_id
        self._address = address
        self._last_updated = last_updated if last_updated is not None else datetime.now(timezone.utc)

    def addEvent(self, event: IPropertyHistoryEvent):
        self._history.append(event)
        self._history.sort(key = lambda event: event._datetime) # Sort by date
        self._last_updated = datetime.now(timezone.utc)

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
    def last_updated(self) -> datetime:
        return self._last_updated

    def __str__(self):
        history_str = "\n".join(str(event) for event in self._history)
        return f"Property ID: {self._property_id},\nAddress: {self._address.get_address_hash()},\nHistory:\n{history_str if history_str else 'No history available'},\nlastUpdated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')}"

# TODO:
# How to deal with vacant land? It has many properties as none, like numberOfBedrooms, numberOfBathrooms, yearBuilt, etc.
# Ready to built home doesn't have year built
class IPropertyBasic:
    def __init__(
        self,
        id: str,
        address: IPropertyAddress,
        area: PropertyArea,
        property_type: PropertyType,
        lot_area: PropertyArea | None,
        number_of_bedrooms: float,
        number_of_bathrooms: float,
        year_built: int | None,
    ):
        self.id: str = id
        self.address = address
        self.area = area
        self.property_type = property_type
        self.lot_area = lot_area
        self.number_of_bedrooms = number_of_bedrooms
        self.number_of_bathrooms = number_of_bathrooms
        self.year_built = year_built
    
    def __str__(self):
        return f"Basic property information: \naddress: {self.address},\nproperty type: {self.property_type.value}, \narea: {self.area}, \nlot area: {self.lot_area}, \nnumberOfBedrooms: {self.number_of_bedrooms}, \nnumberOfBathrooms: {self.number_of_bathrooms}, \nyearBuilt: {self.year_built}"

class PropertyStatus(Enum):
    Active = "Active"
    Pending = "Pending"
    Sold = "Sold"

class IPropertyDataSource:
    def __init__(self, source_id: str, source_url: str, source_name: str):
        self.source_id: str = source_id
        self.source_url: str = source_url
        self.source_name: str = source_name
    
    def __str__(self):
        return f"Name: {self.source_name}, Source ID: {self.source_id}, URL: {self.source_url}"

class IProperty(IPropertyBasic):
    def __init__(
        self,
        id: str,
        address: IPropertyAddress,
        area: PropertyArea,
        property_type: PropertyType,
        lot_area: PropertyArea | None,
        number_of_bedrooms: float,
        number_of_bathrooms: float,
        year_built: int | None,
        status: PropertyStatus,
        price: float | None,
        history: IPropertyHistory,
        last_updated: datetime,
        data_sources: List[IPropertyDataSource] = [],
    ):
        super().__init__(id, address, area, property_type, lot_area, number_of_bedrooms, number_of_bathrooms, year_built)
        self.status = status
        self.price = price
        self.history = history
        self._last_updated = last_updated if last_updated is not None else datetime.now(timezone.utc)
        self.data_sources = data_sources

    @property
    def last_updated(self) -> datetime:
        return self._last_updated

    def __str__(self):
        return (
            super().__str__() +
            f",\nstate: {self.status.value},\nprice: {self.price if self.price is not None else 'N/A'},\ndataSource:\n{",\n".join(str(source)for source in self.data_sources)},\nlastUpdated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')}\nhistory:\n{self.history}\n"
        )

if __name__ == "__main__":
    # Test the IPropertyAddress class
    address = "1838 Market St,Kirkland, WA 98033"
    address_obj = IPropertyAddress(address)
    area = PropertyArea(2879)
    print(address_obj)

    # Test the IPropertyBasic class
    property_id = uuid.uuid4()
    property1 = IPropertyBasic(
        str(property_id),
        IPropertyAddress(address),
        PropertyArea(1700, AreaUnit.SquareFeet),
        PropertyType.SingleFamily,
        area,
        3,
        2.5,
        1899,
    )

    print(property1)