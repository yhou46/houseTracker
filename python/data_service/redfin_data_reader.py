from typing import Iterator, Callable, Any, Dict, Tuple
from enum import Enum
import json
import uuid
from datetime import datetime, timezone, timedelta
import os
from decimal import Decimal

from crawler.redfin_spider.items import RedfinPropertyItem
from shared.iproperty import IProperty, PropertyArea, AreaUnit, PropertyType, PropertyStatus, IPropertyDataSource, IPropertyHistory, PropertyHistoryEventType, IPropertyHistoryEvent, IPropertyMetadata

from shared.iproperty_address import IPropertyAddress
from shared.iproperty_address import InvalidAddressError

class RedfinPropertyEntryTypeCheck:
    def __init__(self):
        for field in RedfinPropertyItem.fields:
            setattr(self, field, None)

class RedfinPropertyEntry:
    def __init__(
            self,
            url: str,
            redfinId: str,
            scrapedAt: str,
            address: str,
            area: str,
            propertyType: str,
            lotArea: str | None,
            numberOfBedrooms: float | None,
            numberOfBathrooms: float | None,
            yearBuilt: int | None,
            status: str,
            price: Decimal | None,
            readyToBuildTag: bool | None,
            ):
        self.url = url
        self.redfinId = redfinId
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

    def __str__(self):
        return f"RedfinPropertyEntry(url={self.url}, redfinId={self.redfinId}, scrapedAt={self.scrapedAt}, address={self.address}, area={self.area}, propertyType={self.propertyType}, lotArea={self.lotArea}, numberOfBedrooms={self.numberOfBedrooms}, numberOfBathrooms={self.numberOfBathrooms}, yearBuilt={self.yearBuilt}, status={self.status}, price={self.price}, readyToBuildTag={self.readyToBuildTag})"

class PropertyDataStreamParsingErrorCode(Enum):
    VacantLandEncountered = "VacantLandEncountered",
    UnknownPropertyType = "UnknownPropertyType",
    UnknownAreaUnit = "UnknownAreaUnit",
    UnknownPropertyStatus = "UnknownPropertyStatus",
    InvalidPropertyDataFormat = "InvalidPropertyDataFormat",
    InvalidPropertyDataType = "InvalidPropertyDataType",
    InvalidPropertyAddress = "InvalidPropertyAddress",
    MissingRequiredField = "MissingRequiredField",


class PropertyDataStreamParsingError(Exception):
    def __init__(self, message: str, original_data: Any, error_code: PropertyDataStreamParsingErrorCode, error_data: Any):
        super().__init__(message)
        self.original_data = original_data
        self.error_code = error_code
        self.error_data = error_data

    def __str__(self):
        return f"{self.__class__.__name__}: message={self.args}, error_code={self.error_code}, original_data={self.original_data}"

PropertyDataStreamErrorHandlerType = Callable[[PropertyDataStreamParsingError], None]

# Placeholder error handler that raises the error
def empty_data_stream_error_handler(error: PropertyDataStreamParsingError) -> None:
   raise error

def parse_datetime_as_utc(datetime_str: str, format: str | None = None) -> datetime:
    """
    Parse scrapedAt timestamp, ensuring it's timezone-aware and in UTC.

    Args:
        datetime_str: datetime string (with or without timezone info)
        format: datetime string format; None means ISO format

    Returns:
        datetime object in UTC timezone
    """
    # Parse the timestamp (works for both timezone-aware and timezone-naive formats)
    dt: datetime = datetime.strptime(datetime_str, format) if format else datetime.fromisoformat(datetime_str)

    if dt.tzinfo is None:
        # Timezone-naive datetime - assume Pacific Time (UTC-8)
        pacific_tz = timezone(timedelta(hours=-8))
        dt = dt.replace(tzinfo=pacific_tz)
        return dt.astimezone(timezone.utc)
    else:
        # Already timezone-aware - convert to UTC if not already
        return dt.astimezone(timezone.utc)

type IPropertyDataStreamIteratorType = tuple[IPropertyMetadata, IPropertyHistory]
class IPropertyDataStream(Iterator[IPropertyDataStreamIteratorType]):

    def __init__(self, error_handler: PropertyDataStreamErrorHandlerType):
        self._error_handler = error_handler

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

class RedfinFileDataReader(IPropertyDataStream):
    def __init__(self, file_path: str, error_handler: PropertyDataStreamErrorHandlerType):
        super().__init__(error_handler)
        self._file_path = file_path
        # self._fileObject: Any = None

    def initialize(self) -> None:
        self._fileObject = open(self._file_path, 'r')
        # Initialize any other resources

    def next_entry(self) -> IPropertyDataStreamIteratorType | None:
        try:
            line = self._fileObject.readline().strip()
            if not line:
                return None

            # Parse line into IProperty
            property_object = parse_json_str_to_property(line)
            # if property_object is None:
            #     return self.next_entry()

            # Parse line into IProperty
            return property_object
        except InvalidAddressError as error:
            error_msg = f"Invalid address encountered: {str(error)}"
            parsing_error = PropertyDataStreamParsingError(
                message = error_msg,
                original_data = line,
                error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyAddress,
                error_data = error.address,
            )
            self._error_handler(parsing_error)
            return self.next_entry()
        except PropertyDataStreamParsingError as e:
            self._error_handler(e)
            return self.next_entry()

    def close(self) -> None:
        if self._fileObject:
            self._fileObject.close()

def validate_redfin_property_entry(entry: RedfinPropertyEntry) -> None:
    if not entry.url or not isinstance(entry.url, str):
        error_msg = f"URL is missing or is not string type: {entry.url}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.url
        )
    if not entry.redfinId or not isinstance(entry.redfinId, str):
        error_msg = f"Redfin ID is missing or is not string type: {entry.redfinId}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.redfinId
        )
    if not entry.address or not isinstance(entry.address, str):
        error_msg = f"Address is missing or is not string type: {entry.address}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.address
        )
    if not entry.scrapedAt or not isinstance(entry.scrapedAt, str):
        error_msg = f"Scraped at timestamp is missing or is not string type: {entry.scrapedAt}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.scrapedAt
        )
    if not entry.area:
        error_msg = f"Area is missing: {entry.area} for address: {entry.address}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.MissingRequiredField,
            error_data = entry.area
        )
    if entry.area and not isinstance(entry.area, str):
        error_msg = f"Area is not string type: {entry.area}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.area
        )
    if not entry.propertyType or not isinstance(entry.propertyType, str):
        error_msg = f"Property type is missing or is not string type: {entry.propertyType}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.propertyType
        )
    if entry.numberOfBedrooms and not isinstance(entry.numberOfBedrooms, (int, float)):
        error_msg = f"Number of bedrooms is not a number: {entry.numberOfBedrooms}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.numberOfBedrooms
        )
    if entry.numberOfBathrooms and not isinstance(entry.numberOfBathrooms, (int, float)):
        error_msg = f"Number of bathrooms is not a number: {entry.numberOfBathrooms}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.numberOfBathrooms
        )
    if not isinstance(entry.yearBuilt, int):
        if entry.readyToBuildTag != True:
            error_msg = f"Year built is missing but property is not marked as ready to build. YearBuilt: {entry.yearBuilt}, readyToBuildTag: {entry.readyToBuildTag}."
            raise PropertyDataStreamParsingError(
                message = error_msg,
                original_data = entry,
                error_code = PropertyDataStreamParsingErrorCode.MissingRequiredField,
                error_data = {
                    "yearBuilt": entry.yearBuilt,
                    "readyToBuildTag": entry.readyToBuildTag
                }
            )
    if not isinstance(entry.status, str):
        error_msg = f"Status is missing or is not string type: {entry.status}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.status
        )
    if entry.price and not isinstance(entry.price, (int, float)):
        error_msg = f"Price is not a number: {entry.price}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.price
        )

def parse_property_history(data: Dict[str, Any], property_id: str, address: IPropertyAddress, last_updated: datetime) -> IPropertyHistory:
    if not isinstance(data, dict):
        raise ValueError("Data must be a dictionary")
    history_list = data.get('history', [])
    property_history: IPropertyHistory = IPropertyHistory(address, [], last_updated)
    for event in history_list:
        if not isinstance(event, dict):
            raise ValueError("Each history event must be a dictionary")

        # Parse date
        date_str = event.get('date')
        if not date_str or not isinstance(date_str, str):
            raise ValueError("Event date is missing or not a string")
        date_obj = parse_datetime_as_utc(date_str, "%b %d, %Y")

        # Parse price
        price = event.get('price')
        if price != None and not isinstance(price, (int, float)):
            raise ValueError("Event price is not a number")

        # Parse event type
        description = event.get('description')
        event_type = PropertyHistoryEventType.Other
        if not isinstance(description, str):
            raise ValueError("Event description is missing or not a string")

        if description.lower().startswith("listed"):
            event_type = PropertyHistoryEventType.Listed
        elif description.lower().startswith("sold"):
            event_type = PropertyHistoryEventType.Sold
        elif description.lower().startswith("price changed"):
            event_type = PropertyHistoryEventType.PriceChange
        elif description.lower().startswith("pending"):
            event_type = PropertyHistoryEventType.Pending
        elif description.lower().startswith("relisted"):
            event_type = PropertyHistoryEventType.ReListed
        elif description.lower().startswith("delisted"):
            event_type = PropertyHistoryEventType.DeListed
        elif description.lower().startswith("contingent"):
            event_type = PropertyHistoryEventType.Contingent
        elif description.lower().startswith("listed for rent"):
            event_type = PropertyHistoryEventType.ListedForRent
        elif description.lower().startswith("rental removed"):
            event_type = PropertyHistoryEventType.RentalRemoved
        elif description.lower().startswith("listing removed"):
            event_type = PropertyHistoryEventType.ListRemoved
        else:
            raise ValueError(f"Unknown event description: {description}")

        if event_type == PropertyHistoryEventType.PriceChange and price is None:
            print(f"Warning: PriceChange event without price on {date_str} for property {property_id}, address {address.address_hash}")

        # Parse source and sourceId
        source = event.get('source')
        if source != None and isinstance(source, str):
            source = source.lower()
        else:
            raise ValueError("Event source is missing or not a string")
        source_id = event.get("mlsNumber")
        if source_id != None and not isinstance(source_id, str):
            source_id = str(source_id)

        # Create event
        event_id = str(uuid.uuid4())
        history_event: IPropertyHistoryEvent = IPropertyHistoryEvent(
            id=event_id,
            datetime=date_obj,
            event_type=event_type,
            description=description,
            source=source,
            source_id=source_id,
            price=price,
        )
        if history_event in property_history.history:
            # Add event to history if not already present
            print(f"Found duplicate event: {history_event} for property {property_id}, address {address}")
        else:
            property_history.addEvent(history_event)

    return property_history

def parse_json_str_to_property(line: str) -> Tuple[IPropertyMetadata, IPropertyHistory]:
    data = json.loads(line)
    redfin_data = RedfinPropertyEntry(
        url=data.get('url'),
        redfinId=data.get('redfinId'),
        scrapedAt=data.get('scrapedAt'),
        address=data.get('address'),
        area=data.get('area'),
        propertyType=data.get('propertyType'),
        lotArea=data.get('lotArea'),
        numberOfBedrooms=data.get('numberOfBedroom'),
        numberOfBathrooms=data.get('numberOfBathroom'),
        yearBuilt=data.get('yearBuilt', None),
        status=data.get('status', 'Unknown'),
        price=data.get('price', None),
        readyToBuildTag=data.get('readyToBuildTag', None),
    )

    property_id = str(uuid.uuid4())

    # Parse property type
    if redfin_data.propertyType == "Townhome":
        property_type = PropertyType.Townhome
    elif redfin_data.propertyType == "Condo":
        property_type = PropertyType.Condo
    elif redfin_data.propertyType == "Single-family":
        property_type = PropertyType.SingleFamily
    elif redfin_data.propertyType == "Vacant land":
        property_type = PropertyType.VacantLand
    elif redfin_data.propertyType == "Multi-family":
        property_type = PropertyType.MultiFamily
    elif redfin_data.propertyType == "Manufactured":
        property_type = PropertyType.Manufactured
    elif redfin_data.propertyType == "Condo (co-op)":
        property_type = PropertyType.Coops
    elif redfin_data.propertyType == "Single Family Residence, 24 - Floating Home/On-Water Res":
        property_type = PropertyType.SingleFamilyOnWater
    else:
        error_msg = f"Unknown property type: {redfin_data.propertyType} for data: {redfin_data.address}"

        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data=line,
            error_code = PropertyDataStreamParsingErrorCode.UnknownPropertyType,
            error_data = redfin_data.propertyType,
        )

    if property_type == PropertyType.VacantLand:
        error_msg = f"Vacant land property detected: {redfin_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data=line,
            error_code = PropertyDataStreamParsingErrorCode.VacantLandEncountered,
            error_data=None,
        )

    validate_redfin_property_entry(redfin_data)

    # Parse address
    address = IPropertyAddress(redfin_data.address)

    # Parse area number and unit
    area_parts = redfin_data.area.split(" ")
    if len(area_parts) != 2:
        error_msg = f"Invalid area format: {redfin_data.area} for address: {redfin_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data=line,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataFormat,
            error_data = redfin_data.area,
        )
    area_number = Decimal(area_parts[0])
    if area_parts[1].lower() == "sqft":
        area_unit = AreaUnit.SquareFeet
    elif area_parts[1].lower() == "acres":
        area_unit = AreaUnit.Acres
    elif area_parts[1].lower() == "sqm2":
        area_unit = AreaUnit.SquareMeter
    else:
        error_msg = f"Unknown area unit: {area_parts[1]} for address: {redfin_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data=line,
            error_code = PropertyDataStreamParsingErrorCode.UnknownAreaUnit,
            error_data = area_parts[1],
        )
    area = PropertyArea(area_number, area_unit)

    # Parse lot area
    lot_area = None
    if redfin_data.lotArea:
        lot_area_parts = redfin_data.lotArea.split(" ")
        if len(lot_area_parts) < 2:
            error_msg = f"Invalid lot area format: {redfin_data.lotArea} for address: {redfin_data.address}"
            raise PropertyDataStreamParsingError(
                message = error_msg,
                original_data=line,
                error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataFormat,
                error_data = redfin_data.lotArea,
            )
        lot_area_number = Decimal(lot_area_parts[0])
        normalized_unit = "".join(lot_area_parts[1:]).lower()
        if normalized_unit == "sqft" or normalized_unit == "squarefeet":
            lot_area_unit = AreaUnit.SquareFeet
        elif normalized_unit == "acres":
            lot_area_unit = AreaUnit.Acres
        elif normalized_unit == "sqm2":
            lot_area_unit = AreaUnit.SquareMeter
        else:
            error_msg = f"Unknown lot area unit: {lot_area_parts[1]} for address: {redfin_data.address}"
            raise PropertyDataStreamParsingError(
                message = error_msg,
                original_data=line,
                error_code = PropertyDataStreamParsingErrorCode.UnknownAreaUnit,
                error_data = lot_area_parts[1],
            )
        lot_area = PropertyArea(lot_area_number, lot_area_unit)

    # Parse number of bedrooms and bathrooms
    number_of_bedrooms = Decimal(redfin_data.numberOfBedrooms) if redfin_data.numberOfBedrooms is not None else None
    number_of_bathrooms = Decimal(redfin_data.numberOfBathrooms) if redfin_data.numberOfBathrooms is not None else None
    year_built = redfin_data.yearBuilt

    # Parse status
    if redfin_data.status == "Active":
        status = PropertyStatus.Active
    elif redfin_data.status == "Pending":
        status = PropertyStatus.Pending
    elif redfin_data.status == "Sold":
        status = PropertyStatus.Sold
    else:
        error_msg = f"Unknown property status: {redfin_data.status} for address: {redfin_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data=line,
            error_code = PropertyDataStreamParsingErrorCode.UnknownPropertyStatus,
            error_data = redfin_data.status,
        )

    # Parse price
    price = redfin_data.price

    # Create data source
    data_source = [
        IPropertyDataSource(
            source_id = redfin_data.redfinId,
            source_url = redfin_data.url,
            source_name = "Redfin"
        )
    ]

    # Parse last update time
    last_updated = parse_datetime_as_utc(redfin_data.scrapedAt, None)

    # Validate some logic
    if property_type != PropertyType.VacantLand and (number_of_bathrooms == None or number_of_bedrooms == None):
        error_msg = f"Number of bedrooms and bathrooms must be provided for non-vacant land properties: {redfin_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            original_data=line,
            error_code = PropertyDataStreamParsingErrorCode.MissingRequiredField,
            error_data = {
                "propertyType": property_type,
                "numberOfBedrooms": number_of_bedrooms,
                "numberOfBathrooms": number_of_bathrooms
            },
        )

    # Parse property history
    history = parse_property_history(data, property_id, address, last_updated)

    # Legacy data doesn't have price
    if price is None and len(history.history) > 0:
        # Set price to the last history event's price
        price = history.history[-1].price


    # Create property object
    property_meta = IPropertyMetadata(
        address = address,
        area = area,
        property_type = property_type,
        lot_area = lot_area,
        number_of_bedrooms = number_of_bedrooms,
        number_of_bathrooms = number_of_bathrooms,
        year_built = year_built,
        status = status,
        price = price,
        last_updated = last_updated,
        data_sources = data_source,
    )
    return property_meta, history

if __name__ == "__main__":
    # Get the directory of the current script (data_reader.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Go up two levels to the project root, then into redfin_output
    python_project_folder = os.path.abspath(os.path.join(current_dir, ".."))
    redfin_output_path = os.path.join(python_project_folder, "crawler", "redfin_output", "redfin_properties_20250821_201954.jsonl")
    print(redfin_output_path)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_log_file = os.path.join(python_project_folder, "data_service", "error_logs", f"data_reader_errors_{timestamp}.log")

    print(f"Starting to read Redfin data from {redfin_output_path}. Error file: {error_log_file}")

    with open(error_log_file, 'w', encoding='utf-8') as error_file:
        def file_error_handler(error: PropertyDataStreamParsingError) -> None:
            error_msg = f"{datetime.now().isoformat()} - {str(error)}\n"
            error_file.write(error_msg)
            error_file.flush()

        reader: IPropertyDataStream = RedfinFileDataReader(redfin_output_path, file_error_handler)
        count = 0

        for metadata, history in reader:
            count += 1
            print(metadata)
            print(history)
            if count % 100 == 0:
                print(f"Processed {count} properties...")
        print(f"Finished processing. Total properties processed: {count}, errors logged to {error_log_file}")
        reader.close()
