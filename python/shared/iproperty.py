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
    def __init__(self, value: float, unit: AreaUnit = AreaUnit.SquareFeet):
        self.value: float = value
        self.unit: AreaUnit = unit

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, PropertyArea):
            return NotImplemented
        return self.value == value.value and self.unit == value.unit

    def __str__(self) -> str:
        return f"{self.value} {self.unit.value}"

class PropertyType(Enum):
    SingleFamily = "SingleFamily"
    SingleFamilyOnWater = "SingleFamilyOnWater"
    Townhome = "Townhome"
    Condo = "Condo"
    VacantLand = "VacantLand"
    MultiFamily = "MultiFamily"
    Manufactured = "Manufactured"
    Coops = "Coops"

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
            id: str,
            datetime: datetime,
            event_type: PropertyHistoryEventType,
            description: str,
            source: str | None = None,
            source_id: str | None = None,
            price: float | None = None,
            ):
        self._id = id
        self._datetime = datetime
        self._event_type = event_type
        self._description = description
        self._price = price
        self._source = source
        self._source_id = source_id

    @property
    def id(self) -> str:
        return self._id
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
    def source(self) -> str | None:
        return self._source
    @property
    def source_id(self) -> str | None:
        return self._source_id

    def __str__(self) -> str:
        return f"Date: {self.datetime.strftime('%Y-%m-%d')}, Event: {self.event_type.value}, Description: {self.description}, Price: {self.price if self.price is not None else 'N/A'}, Source: {self.source if self.source else 'N/A'}, Source ID: {self.source_id if self.source_id else 'N/A'}, id: {self.id}"

    def __eq__(self, other: object) -> bool:
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

    def addEvent(self, event: IPropertyHistoryEvent) -> None:
        self._history.append(event)
        self._history.sort(key = lambda event: event._datetime) # Sort by date
        self._last_updated = datetime.now(timezone.utc)

    @property
    def property_id(self) -> str:
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

    def __str__(self) -> str:
        history_str = "\n".join(str(event) for event in self._history)
        return f"Property ID: {self._property_id},\nAddress: {self._address.get_address_hash()},\nHistory:\n{history_str if history_str else 'No history available'},\nlastUpdated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')}"

class PropertyStatus(Enum):
    Active = "Active"
    Pending = "Pending"
    Sold = "Sold"

class IPropertyDataSource:
    def __init__(self, source_id: str, source_url: str, source_name: str):
        self.source_id: str = source_id
        self.source_url: str = source_url
        self.source_name: str = source_name
    
    def __str__(self) -> str:
        return f"Name: {self.source_name}, Source ID: {self.source_id}, URL: {self.source_url}"

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
    
    def __str__(self) -> str:
        return f"Basic property information: \naddress: {self.address},\nproperty type: {self.property_type.value}, \narea: {self.area}, \nlot area: {self.lot_area}, \nnumberOfBedrooms: {self.number_of_bedrooms}, \nnumberOfBathrooms: {self.number_of_bathrooms}, \nyearBuilt: {self.year_built}"

class IPropertyMetadata(IPropertyBasic):
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
        last_updated: datetime,
        data_sources: List[IPropertyDataSource] = [],
    ):
        super().__init__(id, address, area, property_type, lot_area, number_of_bedrooms, number_of_bathrooms, year_built)
        self._status = status
        self._price = price
        self._last_updated = last_updated if last_updated is not None else datetime.now(timezone.utc)
        self._data_sources = data_sources

    @property
    def status(self) -> PropertyStatus:
        return self._status

    @property
    def price(self) -> float | None:
        return self._price

    @property
    def last_updated(self) -> datetime:
        return self._last_updated

    @property
    def data_sources(self) -> List[IPropertyDataSource]:
        return self._data_sources

    def __str__(self) -> str:
        return (
            super().__str__() +
            f",\nstate: {self._status.value},\nprice: {self._price if self._price is not None else 'N/A'},\ndataSource:\n{",\n".join(str(source)for source in self._data_sources)},\nlastUpdated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')}\n"
        )

class IProperty():
    def __init__(
        self,
        property_metadata: IPropertyMetadata,
        property_history: IPropertyHistory,
    ):
        self._metadata = property_metadata
        self._history = property_history

    @property
    def id(self) -> str:
        return self._metadata.id
    
    @property
    def address(self) -> IPropertyAddress:
        return self._metadata.address

    @property
    def area(self) -> PropertyArea:
        return self._metadata.area

    @property
    def property_type(self) -> PropertyType:
        return self._metadata.property_type

    @property
    def lot_area(self) -> PropertyArea | None:
        return self._metadata.lot_area

    @property
    def number_of_bedrooms(self) -> float:
        return self._metadata.number_of_bedrooms

    @property
    def number_of_bathrooms(self) -> float:
        return self._metadata.number_of_bathrooms

    @property
    def year_built(self) -> int | None:
        return self._metadata.year_built

    @property
    def status(self) -> PropertyStatus:
        return self._metadata._status

    @property
    def last_updated(self) -> datetime:
        return self._metadata.last_updated

    @property
    def price(self) -> float | None:
        return self._metadata._price

    @property
    def data_sources(self) -> List[IPropertyDataSource]:
        return self._metadata._data_sources

    @property
    def history(self) -> IPropertyHistory:
        return self._history

    def __str__(self) -> str:
        return (
            super().__str__() +
            f",\nstate: {self.status.value},\nprice: {self.price if self.price is not None else 'N/A'},\ndataSource:\n{",\n".join(str(source)for source in self.data_sources)},\nlastUpdated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')}\nhistory:\n{self._history}\n"
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