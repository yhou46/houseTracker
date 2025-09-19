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

"""
The relationship of each argument is AND
The relationship of each element within the argument is OR
"""
class PropertyQueryPattern:
    def __init__(
        self,
        state: str,
        zip_code_list: List[int] | None = None, # must have at least zip code or city
        city_list: List[str] | None = None,
        property_type_list: List[PropertyType] | None = None,
        status_list: List[PropertyStatus] | None = None,
        event_type_list: List[PropertyHistoryEventType] | None = None,
        even_type_time_range: Tuple[datetime, datetime] | None = None,
        price_range: Tuple[Decimal, Decimal] | None = None,
        number_of_bedrooms_range: Tuple[Decimal, Decimal] | None = None,
        number_of_bathrooms_range: Tuple[Decimal, Decimal] | None = None,
        ):
        # if not zip_code_list and not city_list:
        #     raise ValueError("zip_code and city cannot be empty at the same time")
        self.state = state
        self.zip_code_list = zip_code_list
        self.city_list = city_list
        self.property_type_list = property_type_list
        self.status_list = status_list
        self.event_type_list = event_type_list
        self.even_type_time_range = even_type_time_range
        self.price_range = price_range
        self.number_of_bedrooms_range = number_of_bedrooms_range
        self.number_of_bathrooms_range = number_of_bathrooms_range

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
    @abstractmethod
    def close(self) -> None:
        pass