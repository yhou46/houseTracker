from typing import (
    Iterator,
    Callable,
    Any,
    Dict,
    Literal,
    List,
    Set,
    Tuple,
    Optional,
    cast,
)
from decimal import Decimal

'''

'''
class RawPropertyData:
    def __init__(
            self,
            url: str,
            data_source_name: Literal["Redfin", "Zillow"],
            data_source_id: str,
            scrapedAt: str,
            address: str,
            area: str | None,
            propertyType: str | None,
            lotArea: str | None,
            numberOfBedrooms: float | None,
            numberOfBathrooms: float | None,
            yearBuilt: int | None,
            status: str,
            price: Decimal | None,
            readyToBuildTag: bool | None,
            history: List[Any] = [],
            ):
        self.url = url
        self.data_source_name = data_source_name
        self.data_source_id = data_source_id
        self.scrapedAt = scrapedAt
        self.address = address
        self.area = area
        self.propertyType = propertyType
        self.lotArea = lotArea
        self.numberOfBedrooms = numberOfBedrooms
        self.numberOfBathrooms = numberOfBathrooms
        self.yearBuilt = yearBuilt
        self.status = status
        self.price = price
        self.readyToBuildTag = readyToBuildTag
        self.history = history

    def __str__(self) -> str:
        return f"RawProertyData(url={self.url}, data_source_name={self.data_source_name}, data_source_id={self.data_source_id}, scrapedAt={self.scrapedAt}, address={self.address}, area={self.area}, propertyType={self.propertyType}, lotArea={self.lotArea}, numberOfBedrooms={self.numberOfBedrooms}, numberOfBathrooms={self.numberOfBathrooms}, yearBuilt={self.yearBuilt}, status={self.status}, price={self.price}, readyToBuildTag={self.readyToBuildTag})"

type IPropertyDataStreamIteratorType = RawPropertyData

PropertyDataStreamErrorHandlerType = Callable[[Any], None]

class IPropertyDataStream(Iterator[IPropertyDataStreamIteratorType]):

    def __init__(self, error_handler: Optional[PropertyDataStreamErrorHandlerType]) -> None:
        self._error_handler = error_handler

    # TODO: it probably should return a json typed dict, since the parsing need extra info, like existing property data in DB to determine some fields
    def __iter__(self) -> Iterator[IPropertyDataStreamIteratorType]:
        self.initialize()
        return self

    def __next__(self) -> IPropertyDataStreamIteratorType:
        entry = self.next_entry()
        if entry is None:
            self.close()
            raise StopIteration
        return entry

    '''
    Should return None when there are no more entries.
    Raise exceptions for errors, which will be handled by the error handler.
    '''
    def next_entry(self) -> IPropertyDataStreamIteratorType | None:
        raise NotImplementedError("This method should be overridden by subclasses")

    def initialize(self) -> None:
        raise NotImplementedError("This method should be overridden by subclasses")

    def close(self) -> None:
        raise NotImplementedError("This method should be overridden by subclasses")