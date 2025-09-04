from enum import Enum
from datetime import datetime, timezone
from typing import List
import math
from decimal import Decimal

import uuid
import logging

from shared.iproperty_address import IPropertyAddress


class AreaUnit(Enum):
    SquareFeet = "SquareFeet"
    SquareMeter = "SquareMeter"
    Acres = "Acres"

class PropertyArea:
    def __init__(self, value: Decimal, unit: AreaUnit = AreaUnit.SquareFeet):
        self.value: Decimal = value
        self.unit: AreaUnit = unit

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, PropertyArea):
            return False
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
    ListRemoved = "ListRemoved"
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
            price: Decimal | None = None,
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
    def price(self) -> Decimal | None:
        return self._price
    @property
    def source(self) -> str | None:
        return self._source
    @property
    def source_id(self) -> str | None:
        return self._source_id

    def __str__(self) -> str:
        return f"Date: {self.datetime.isoformat()}, Event: {self.event_type.value}, Description: {self.description}, Price: {self.price if self.price is not None else 'N/A'}, Source: {self.source if self.source else 'N/A'}, Source ID: {self.source_id if self.source_id else 'N/A'}, id: {self.id}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IPropertyHistoryEvent):
            return False
        is_same_price = False
        if self.price is None or other.price is None:
            is_same_price = (self.price == other.price)
        else:
            is_same_price = math.isclose(self.price, other.price)
        return (self._datetime == other._datetime and
                self._event_type == other._event_type and
                is_same_price and
                self._source == other._source and
                self._source_id == other._source_id)

    def __lt__(self, other: object) -> bool:
        """Enable sorting by: datetime -> event_type -> price -> source -> source_id -> description"""
        if not isinstance(other, IPropertyHistoryEvent):
            return NotImplemented

        # Compare datetime first
        if self._datetime != other._datetime:
            return self._datetime < other._datetime

        # If datetime is equal, compare event_type
        if self._event_type != other._event_type:
            return self._event_type.value < other._event_type.value

        # If event_type is equal, compare price
        if self._price != other._price:
            # Handle None values - None is considered less than any number
            if self._price is None:
                return True
            if other._price is None:
                return False
            return self._price < other._price

        # If price is equal, compare source
        if self._source != other._source:
            # Handle None values - None is considered less than any string
            if self._source is None:
                return True
            if other._source is None:
                return False
            return self._source < other._source

        # If source is equal, compare source_id
        if self._source_id != other._source_id:
            # Handle None values - None is considered less than any string
            if self._source_id is None:
                return True
            if other._source_id is None:
                return False
            return self._source_id < other._source_id

        # If source_id is equal, compare description
        return self._description < other._description

    def __le__(self, other: object) -> bool:
        if not isinstance(other, IPropertyHistoryEvent):
            return NotImplemented
        return self < other or self == other

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, IPropertyHistoryEvent):
            return NotImplemented
        return not (self <= other)

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, IPropertyHistoryEvent):
            return NotImplemented
        return not (self < other)

# All prices are in USD
class IPropertyHistory:
    def __init__(
            self,
            address: IPropertyAddress,
            history: List[IPropertyHistoryEvent],
            last_updated: datetime,
            ):
        self._history = history if history is not None else []
        self._address = address
        self._last_updated = last_updated

        # Sort history event
        self._history.sort() # Now uses natural sorting via __lt__ method

    def addEvent(self, event: IPropertyHistoryEvent) -> None:
        self._history.append(event)
        self._history.sort() # Now uses natural sorting via __lt__ method

    @property
    def address(self) -> IPropertyAddress:
        return self._address

    @property
    def history(self) -> list[IPropertyHistoryEvent]:
        return self._history

    @property
    def last_updated(self) -> datetime:
        return self._last_updated

    @staticmethod
    def merge_history(existing_history: "IPropertyHistory", new_history: "IPropertyHistory") -> "IPropertyHistory":
        """
        Merge two IPropertyHistory objects, new events from new_history will be added to existing_history.
        Existing events in existing_history is unchanged and duplicates from new_history will be removed and e
        """
        if existing_history.address != new_history.address:
            raise ValueError("Cannot merge histories with different addresses")

        # The order matters: existing_history should be first to keep existing events
        combined_events = existing_history.history + new_history.history
        unique_events = []
        for event in combined_events:
            # TODO: there is one case: some events are missing some fields but later the field gets added
            # Example: PriceChange event,  price is missing, later price is added. In this case, we want to have only 1 event instead of 2
            if event not in unique_events:
                unique_events.append(event)
        # Sort by event datetime using natural ordering
        unique_events.sort()
        # Use the latest last_updated
        last_updated = max(existing_history.last_updated, new_history.last_updated)
        return IPropertyHistory(
            address=existing_history.address,
            history=unique_events,
            last_updated=last_updated,
        )

    def __str__(self) -> str:
        history_str = "\n".join(str(event) for event in self._history)
        return f"Address: {self._address.address_hash},\nHistory:\n{history_str if history_str else 'No history available'},\nlastUpdated: {self.last_updated.isoformat()}"

    def __eq__(self, value) -> bool:
        if not isinstance(value, IPropertyHistory):
            return False

        # Check other properties
        if self.address != value.address or self.last_updated != value.last_updated:
            return False

        # Check each entry in history
        if len(self.history) != len(value.history):
            return False
        i = 0
        for i in range(len(self.history)):
            if self.history[i] != value.history[i]:
                return False
        return True

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IPropertyDataSource):
            return False
        return (self.source_id == other.source_id and
                self.source_url == other.source_url and
                self.source_name == other.source_name)

# TODO:
# How to deal with vacant land? It has many properties as none, like numberOfBedrooms, numberOfBathrooms, yearBuilt, etc.
# Ready to built home doesn't have year built
class IPropertyBasic:
    def __init__(
        self,
        address: IPropertyAddress,
        area: PropertyArea,
        property_type: PropertyType,
        lot_area: PropertyArea | None,
        number_of_bedrooms: Decimal,
        number_of_bathrooms: Decimal,
        year_built: int | None,
    ):
        self.address = address
        self.area = area
        self.property_type = property_type
        self.lot_area = lot_area
        self.number_of_bedrooms = number_of_bedrooms
        self.number_of_bathrooms = number_of_bathrooms
        self.year_built = year_built

    def __str__(self) -> str:
        return f"Basic property information: \naddress: {self.address},\nproperty type: {self.property_type.value}, \narea: {self.area}, \nlot area: {self.lot_area}, \nnumberOfBedrooms: {self.number_of_bedrooms}, \nnumberOfBathrooms: {self.number_of_bathrooms}, \nyearBuilt: {self.year_built}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IPropertyBasic):
            return False
        return (self.address == other.address and
                self.area == other.area and
                self.property_type == other.property_type and
                self.lot_area == other.lot_area and
                self.number_of_bedrooms == other.number_of_bedrooms and
                self.number_of_bathrooms == other.number_of_bathrooms and
                self.year_built == other.year_built)

# DB layer property metadata
class IPropertyMetadata(IPropertyBasic):
    def __init__(
        self,
        address: IPropertyAddress,
        area: PropertyArea,
        property_type: PropertyType,
        lot_area: PropertyArea | None,
        number_of_bedrooms: Decimal,
        number_of_bathrooms: Decimal,
        year_built: int | None,
        status: PropertyStatus,
        price: Decimal | None,
        last_updated: datetime,
        data_sources: List[IPropertyDataSource] = [],
    ):
        super().__init__(address, area, property_type, lot_area, number_of_bedrooms, number_of_bathrooms, year_built)
        self._status = status
        self._price = price
        self._last_updated = last_updated
        self._data_sources = data_sources

    @property
    def status(self) -> PropertyStatus:
        return self._status

    @property
    def price(self) -> Decimal | None:
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
            f",\nstatus: {self._status.value},\nprice: {self._price if self._price is not None else 'N/A'},\ndataSource:\n{",\n".join(str(source)for source in self._data_sources)},\nlastUpdated: {self.last_updated.isoformat()}"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IPropertyMetadata):
            return False
        is_same_price = False
        if self.price is None or other.price is None:
            is_same_price = (self.price == other.price)
        else:
            is_same_price = math.isclose(self.price, other.price)
        return (super().__eq__(other) and
                self._status == other._status and
                is_same_price and
                self._last_updated == other._last_updated and
                self._data_sources == other._data_sources)

# DB layer property
class IProperty():
    def __init__(
        self,
        id: str,
        property_metadata: IPropertyMetadata,
        property_history: IPropertyHistory,
    ):
        self._id = id
        self._metadata = property_metadata
        self._history = property_history

    @property
    def id(self) -> str:
        return self._id

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
    def number_of_bedrooms(self) -> Decimal:
        return self._metadata.number_of_bedrooms

    @property
    def number_of_bathrooms(self) -> Decimal:
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
    def price(self) -> Decimal | None:
        return self._metadata._price

    @property
    def data_sources(self) -> List[IPropertyDataSource]:
        return self._metadata._data_sources

    @property
    def metadata(self) -> IPropertyMetadata:
        return self._metadata

    @property
    def history(self) -> IPropertyHistory:
        return self._history

    def update_metadata(self, new_metadata: IPropertyMetadata):
        if self._metadata.last_updated < new_metadata.last_updated:
            self._metadata = new_metadata

    def update_history(self, new_history: IPropertyHistory):
        self._history = IPropertyHistory.merge_history(self._history, new_history)

    def __str__(self) -> str:
        return (
            f"id: {self.id}\n" +
            self._metadata.__str__() +
            f"\nHistory:\n{self._history.__str__()}"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IProperty):
            return False
        return (self._metadata == other._metadata and
                self._history == other._history)

    @staticmethod
    def generate_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def compare_print_diff(property1: "IProperty", property2: "IProperty"):
        """
        Compare two IProperty objects and print differences in their fields.

        Args:
            property1: First IProperty object to compare
            property2: Second IProperty object to compare
        """
        print("=== Comparing IProperty objects ===")

        # Compare metadata fields
        meta1 = property1.metadata
        meta2 = property2.metadata

        # Address comparison
        if meta1.address != meta2.address:
            print(f"Address is different: {meta1.address} != {meta2.address}")

        # Area comparison
        if meta1.area != meta2.area:
            print(f"Area is different: {meta1.area} != {meta2.area}")

        # Property type comparison
        if meta1.property_type != meta2.property_type:
            print(f"Property type is different: {meta1.property_type.value} != {meta2.property_type.value}")

        # Lot area comparison
        if meta1.lot_area != meta2.lot_area:
            print(f"Lot area is different: {meta1.lot_area} != {meta2.lot_area}")

        # Number of bedrooms comparison
        if meta1.number_of_bedrooms != meta2.number_of_bedrooms:
            print(f"Number of bedrooms is different: {meta1.number_of_bedrooms} != {meta2.number_of_bedrooms}")

        # Number of bathrooms comparison
        if meta1.number_of_bathrooms != meta2.number_of_bathrooms:
            print(f"Number of bathrooms is different: {meta1.number_of_bathrooms} != {meta2.number_of_bathrooms}")

        # Year built comparison
        if meta1.year_built != meta2.year_built:
            print(f"Year built is different: {meta1.year_built} != {meta2.year_built}")

        # Status comparison
        if meta1.status != meta2.status:
            print(f"Status is different: {meta1.status.value} != {meta2.status.value}")

        # Price comparison (handle None values and Decimaling point precision)
        price1 = meta1.price
        price2 = meta2.price
        if price1 is None and price2 is None:
            pass  # Both None, no difference
        elif price1 is None or price2 is None:
            print(f"Price is different: {price1} != {price2}")
        elif not math.isclose(price1, price2):
            print(f"Price is different: {price1} != {price2}")

        # Last updated comparison
        if meta1.last_updated != meta2.last_updated:
            print(f"Last updated is different: {meta1.last_updated} != {meta2.last_updated}")

        # Data sources comparison
        if meta1.data_sources != meta2.data_sources:
            print(f"Data sources are different:")
            print(f"  Property1 sources: {[str(source) for source in meta1.data_sources]}")
            print(f"  Property2 sources: {[str(source) for source in meta2.data_sources]}")

        # History comparison
        history1 = property1.history
        history2 = property2.history

        if history1.address != history2.address:
            print(f"History address is different: {history1.address} != {history2.address}")

        if history1.last_updated != history2.last_updated:
            print(f"History last updated is different: {history1.last_updated} != {history2.last_updated}")

        if len(history1.history) != len(history2.history):
            print(f"History count is different: {len(history1.history)} != {len(history2.history)}")
        else:
            # Compare individual history events
            for i, (event1, event2) in enumerate(zip(history1.history, history2.history)):
                if event1 != event2:
                    print(f"History event {i} is different:")
                    print(f"  Event1: {event1}")
                    print(f"  Event2: {event2}")

        print("=== Comparison complete ===")

# def merge_property(
#     property1: IProperty,
#     property2: IProperty,
# ) -> IProperty:
#     """
#     Merge the old property with the new property metadata.
#     This function will update the metadata and history of the old property with the new property.
#     """
#     # Update metadata
#     new_history = IPropertyHistory.merge_history(
#         property1.history,
#         property2.history,
#     )
#     new_property_metadata = property2.metadata if property2.last_updated > property1.metadata.last_updated else property1.metadata
#     return IProperty(
#         property_metadata=new_property_metadata,
#         property_history=new_history,
#     )

if __name__ == "__main__":
    # Test the IPropertyAddress class
    address = "1838 Market St,Kirkland, WA 98033"
    address_obj = IPropertyAddress(address)
    area = PropertyArea(Decimal(2879))
    print(address_obj)

    # Test the IPropertyBasic class
    property1 = IPropertyBasic(
        IPropertyAddress(address),
        PropertyArea(Decimal(1700), AreaUnit.SquareFeet),
        PropertyType.SingleFamily,
        area,
        Decimal(3),
        Decimal(2.5),
        1899,
    )

    print(property1)