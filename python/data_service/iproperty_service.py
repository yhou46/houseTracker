from abc import ABC, abstractmethod
from typing import (
    List,
    Tuple,
    Any,
)
from decimal import Decimal
from datetime import datetime

from shared.iproperty import (
    IProperty,
    PropertyType,
    PropertyStatus,
    IPropertyMetadata,
    IPropertyHistory,
    PropertyHistoryEventType,
)
from shared.iproperty_address import IPropertyAddress

class PropertyQueryPattern:
    def __init__(
        self,
        state: str,
        zip_code: List[int] | None, # must have at least zip code or city
        city: List[str] | None,
        property_type: List[PropertyType] | None,
        status: List[PropertyStatus] | None,
        event_type: List[PropertyHistoryEventType] | None,
        even_type_time_range: Tuple[datetime, datetime] | None,
        price_range: Tuple[Decimal, Decimal] | None,
        number_of_bedrooms: Tuple[Decimal, Decimal] | None,
        number_of_bathrooms: Tuple[Decimal, Decimal] | None,
        ):
        if not zip_code and not city:
            raise ValueError("zip_code and city cannot be empty at the same time")
        if zip_code and city:
            raise ValueError("cannot provide zip_code and city at the same time")
        self.state = state
        self.zip_code = zip_code
        self.city = city
        self.property_type = property_type
        self.status = status
        self.event_type = event_type
        self.even_type_time_range = even_type_time_range
        self.price_range = price_range
        self.number_of_bedrooms = number_of_bedrooms
        self.number_of_bathrooms = number_of_bathrooms

type IPropertyServiceLastEvaluateKeyType = Any

class IPropertyService(ABC):
    """
    Read interfaces
    """
    @abstractmethod
    def get_property_by_id(self, property_id: str) -> IProperty | None:
        pass

    @abstractmethod
    def get_property_by_address(self, address_obj: IPropertyAddress) -> IProperty | None:
        pass

    # TODO: need to return part of the result if the total number of results is too large
    @abstractmethod
    def query_properties(
        self,
        query: PropertyQueryPattern,
        limit: int,
        exclusive_start_key: IPropertyServiceLastEvaluateKeyType,
        ) -> Tuple[List[IProperty], IPropertyServiceLastEvaluateKeyType | None]:
        """
        Return a list of properties that match the query pattern
        1. If the number of results is more than limit, return limit number of results, with exclusive_start_key returned
        2. If the number of results is less than limit, return all results, with exclusive_start_key set to None
        """
        pass

    """
    Write interfaces
    """

    @abstractmethod
    def create_or_update_property(self, property_metadata: IPropertyMetadata, property_history: IPropertyHistory) -> IProperty:
        pass

    @abstractmethod
    def delete_property_by_id(self, property_id: str) -> None:
        pass

    """
    Other
    """