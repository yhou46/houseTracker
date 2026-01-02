from typing import (
    Any,
    Tuple,
    Dict,
    Set,
)
from enum import Enum
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import uuid

from data_service.iproperty_data_reader import (
    RawPropertyData,
)
import shared.logger_factory as logger_factory
from shared.iproperty import (
    PropertyArea,
    AreaUnit,
    IProperty,
    PropertyType,
    PropertyStatus,
    IPropertyDataSource,
    IPropertyHistory,
    PropertyHistoryEventType,
    IPropertyHistoryEvent,
    IPropertyMetadata,
)
from shared.iproperty_address import IPropertyAddress, InvalidAddressError

# def parse_raw_data_to_property(raw_property_data: RawPropertyData, existing_property: IProperty) -> Tuple[IPropertyMetadata, IPropertyHistory]:
#     """
#     Parse RawPropertyData to IPropertyMetadata and List of IPropertyHistoryEntry
#     """
#     pass

class PropertyDataStreamParsingErrorCode(Enum):
    VacantLandEncountered = "VacantLandEncountered",
    UnknownPropertyType = "UnknownPropertyType",
    UnknownAreaUnit = "UnknownAreaUnit",
    UnknownPropertyStatus = "UnknownPropertyStatus",
    InvalidPropertyDataFormat = "InvalidPropertyDataFormat",
    InvalidPropertyDataType = "InvalidPropertyDataType",
    InvalidPropertyAddress = "InvalidPropertyAddress",
    MissingRequiredField = "MissingRequiredField",
    ReadyToBuildTagEncountered = "ReadyToBuildTagEncountered",

class PropertyDataStreamParsingError(Exception):
    def __init__(self, message: str, input_data: Any, error_code: PropertyDataStreamParsingErrorCode, error_data: Any):
        super().__init__(message)
        self.input_data = input_data
        self.error_code = error_code
        self.error_data = error_data

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: message={self.args}, error_code={self.error_code}, original_data={self.input_data}"

def validate_redfin_property_entry(entry: RawPropertyData) -> None:
    if not entry.url or not isinstance(entry.url, str):
        error_msg = f"URL is missing or is not string type: {entry.url}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.url
        )

    if not entry.data_source_name or entry.data_source_name != "Redfin":
        error_msg = f"Data source name is missing or is not 'Redfin': {entry.data_source_name}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.data_source_name
        )

    if not entry.data_source_id or not isinstance(entry.data_source_id, str):
        error_msg = f"Data source id is missing or is not string type: {entry.data_source_id}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.data_source_id
        )
    if not entry.address or not isinstance(entry.address, str):
        error_msg = f"Address is missing or is not string type: {entry.address}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.address
        )
    if not entry.scrapedAt or not isinstance(entry.scrapedAt, str):
        error_msg = f"Scraped at timestamp is missing or is not string type: {entry.scrapedAt}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.scrapedAt
        )
    if not entry.area:
        error_msg = f"Area is missing: {entry.area} for address: {entry.address}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.MissingRequiredField,
            error_data = entry.area
        )
    if entry.area and not isinstance(entry.area, str):
        error_msg = f"Area is not string type: {entry.area}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.area
        )
    if not entry.propertyType or not isinstance(entry.propertyType, str):
        error_msg = f"Property type is missing or is not string type: {entry.propertyType}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.propertyType
        )
    if entry.numberOfBedrooms and not isinstance(entry.numberOfBedrooms, (int, float)):
        error_msg = f"Number of bedrooms is not a number: {entry.numberOfBedrooms}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.numberOfBedrooms
        )
    if entry.numberOfBathrooms and not isinstance(entry.numberOfBathrooms, (int, float)):
        error_msg = f"Number of bathrooms is not a number: {entry.numberOfBathrooms}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.numberOfBathrooms
        )
    if not isinstance(entry.yearBuilt, int):
        if entry.readyToBuildTag != True:
            error_msg = f"Year built is missing but property is not marked as ready to build. YearBuilt: {entry.yearBuilt}, readyToBuildTag: {entry.readyToBuildTag}."
            raise PropertyDataStreamParsingError(
                message = error_msg,
                input_data = entry,
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
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.status
        )
    if entry.price and not isinstance(entry.price, (int, float)):
        error_msg = f"Price is not a number: {entry.price}."
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = entry,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataType,
            error_data = entry.price
        )

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
        pacific_tz = ZoneInfo("America/Los_Angeles")
        dt = dt.replace(tzinfo=pacific_tz)
        return dt.astimezone(timezone.utc)
    else:
        # Already timezone-aware - convert to UTC if not already
        return dt.astimezone(timezone.utc)

def parse_property_history(
        data: RawPropertyData,
        address: IPropertyAddress,
        last_updated: datetime,
        ) -> IPropertyHistory:

    logger = logger_factory.get_logger(__name__)

    history_list = data.history
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
            # Found rent related events
            if description.lower().find("rent") != -1:
                event_type = PropertyHistoryEventType.ListedForRent
            else:
                event_type = PropertyHistoryEventType.Listed
        elif description.lower().startswith("sold"):
            event_type = PropertyHistoryEventType.Sold
        elif description.lower().startswith("price changed"):
            # TODO: need to tell between sale and rent
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
            logger.warning(f"Warning: PriceChange event without price on {date_str} for property, address {address.address_hash}")

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
        if history_event not in property_history.history:
            # Add event to history if not already present
            property_history.addEvent(history_event)
        else:
            logger.warning(f"Found duplicate event: {history_event} for property, address {address}")

    return property_history

# Update this map for new status string
_property_status_value_map: Dict[str, PropertyStatus] = {
    # Active status
    "for sale": PropertyStatus.Active,

    # Pending status
    "pending": PropertyStatus.Pending,

    # Sold status
    "sold": PropertyStatus.Sold,

    # Off market status, not sold but withdrawn by owner
    "off market": PropertyStatus.ListRemoved,

    # Rental removed
    "rental removed": PropertyStatus.RentalRemoved,

    # Rental listed
    "for rent": PropertyStatus.ActiveForRental,
}

def parse_property_status(status_str: str, history: IPropertyHistory) -> PropertyStatus:

    logger = logger_factory.get_logger(__name__)

    # Handle cases like "pending - backup offer requested"
    entries = status_str.split("-")
    status = _property_status_value_map.get(entries[0].strip())

    if status:
        return status

    if not status:
        if status_str.startswith(PropertyStatus.Sold.value):
            return PropertyStatus.Sold

        # use history to determine the status
        history_events = history.history
        if status_str.startswith("off market— sold"):
            """
            Example: https://www.redfin.com/WA/Bellevue/14651-NE-40th-St-98007/unit-C4/home/25631
            This one's redfin status is OFF MARKET— SOLD JUL 2021 FOR $525,000, but was listed before in DB record, in this case, the status should be list removed, which mean the property is not sold and withdraw by the owner
            """
            event_type_set: Set[PropertyHistoryEventType] = {
                PropertyHistoryEventType.Listed,
                PropertyHistoryEventType.ReListed,
                PropertyHistoryEventType.DeListed,
                PropertyHistoryEventType.PriceChange,
                PropertyHistoryEventType.RentalRemoved,
                PropertyHistoryEventType.ListRemoved,
                PropertyHistoryEventType.Pending,
            }
            if len(history_events) > 0 and (history_events[-1].event_type in event_type_set):
                return PropertyStatus.ListRemoved

        # Handle cases like "soldon aug 5, 2025"
        if status_str.startswith("sold"):
            event_type_set_for_sold: Set[PropertyHistoryEventType] = {
                PropertyHistoryEventType.Sold,
                PropertyHistoryEventType.DeListed,
            }

            def parse_soldon_date(date_str: str) -> datetime:
                """
                Parse a string in the format 'soldon aug 5, 2025' and return a datetime object in Pacific Time.

                Args:
                    date_str: The input string in the format 'soldon <month> <day>, <year>'.

                Returns:
                    A datetime object in Pacific Time.
                """
                # Handle special case: "soldon today" or "soldon yesterday"
                if date_str.lower() == "soldtoday":
                    today = datetime.now(ZoneInfo("America/Los_Angeles"))
                    return today
                if date_str.lower() == "soldyesterday":
                    yesterday = (datetime.now(ZoneInfo("America/Los_Angeles")) - timedelta(days=1))
                    return yesterday

                # Remove the "soldon" prefix and strip any extra whitespace
                date_part = date_str.replace("soldon", "").strip()

                # Parse the date part into a naive datetime object
                naive_date = datetime.strptime(date_part, "%b %d, %Y")

                # Assign the Pacific Timezone
                pacific_time = naive_date.replace(tzinfo=ZoneInfo("America/Los_Angeles"))

                return pacific_time

            sold_date = parse_soldon_date(status_str)

            if len(history_events) > 0:
                two_days = timedelta(days=2)
                seven_days = timedelta(days=7)
                if history_events[-1].event_type in event_type_set_for_sold and (abs(sold_date - history_events[-1].datetime) <= seven_days):
                    return PropertyStatus.Sold

                if history_events[-1].event_type == PropertyHistoryEventType.Pending:

                    # Check previous event if it is sold; Sometimes pending event is added after sold event
                    # If pending and sold event are within 2 days, consider it as sold
                    if len(history_events) > 1 and history_events[-2].event_type == PropertyHistoryEventType.Sold and history_events[-1].datetime - history_events[-2].datetime <= two_days:
                        return PropertyStatus.Sold

                if history_events[-1].event_type == PropertyHistoryEventType.RentalRemoved:
                    return PropertyStatus.RentalRemoved

                if history_events[-1].event_type == PropertyHistoryEventType.Listed:
                    return PropertyStatus.ListRemoved

                if history_events[-1].datetime < sold_date:
                    return PropertyStatus.ListRemoved

            logger.warning(f"Warning: Unable to determine sold status from history for status string: {status_str}, defaulting to Sold")
            return PropertyStatus.Sold

        if status_str.startswith("closed"):
            # Property is not in market, need to check history to determine if it is rental or sale closed
            for event in reversed(history_events):
                if event.event_type == PropertyHistoryEventType.ListedForRent or event.description.lower().find("rent") != -1:
                    return PropertyStatus.RentalRemoved

                if event.event_type == PropertyHistoryEventType.Listed and event.description.lower().find("rent") == -1:
                    return PropertyStatus.ListRemoved

    error_msg = f"Failed to parse status string: {status_str}"
    raise PropertyDataStreamParsingError(
        message = error_msg,
        input_data=status_str,
        error_code = PropertyDataStreamParsingErrorCode.UnknownPropertyStatus,
        error_data = status_str,
    )

def parse_property_type(property_type_str: str | None) -> PropertyType:
    if property_type_str == "Townhome":
        return PropertyType.Townhome
    elif property_type_str == "Condo":
        return PropertyType.Condo
    elif property_type_str == "Single-family":
        return PropertyType.SingleFamily
    elif property_type_str == "Vacant land":
        return PropertyType.VacantLand
    elif property_type_str == "Multi-family":
        return PropertyType.MultiFamily
    elif property_type_str == "Manufactured":
        return PropertyType.Manufactured
    elif property_type_str == "Condo (co-op)":
        return PropertyType.Coops
    elif property_type_str == "Single Family Residence, 24 - Floating Home/On-Water Res":
        return PropertyType.SingleFamilyOnWater
    else:
        error_msg = f"Unknown property type: {property_type_str}"

        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = property_type_str,
            error_code = PropertyDataStreamParsingErrorCode.UnknownPropertyType,
            error_data = property_type_str,
        )

# TODO: complete this function
# Currently only update history for ListRemoved status
def correct_property_history(
        old_status: PropertyStatus,
        new_status: PropertyStatus,
        history: IPropertyHistory,
        last_updated: datetime,
        ) -> None:
    """
    Correct property history based on status change
    """
    logger = logger_factory.get_logger(__name__)

    if new_status == PropertyStatus.ListRemoved:
        should_add_event = False
        recent_list_event_date = None
        recent_list_event_index = -1
        recent_list_removed_event_date = None
        for i in range(len(history.history)-1, -1, -1):
            event = history.history[i]
            if event.event_type == PropertyHistoryEventType.Listed:
                recent_list_event_date = event.datetime if recent_list_event_date is None else recent_list_event_date
                recent_list_event_index = i
            if event.event_type == PropertyHistoryEventType.ListRemoved:
                recent_list_removed_event_date = event.datetime if recent_list_removed_event_date is None else recent_list_removed_event_date
            if recent_list_event_date and recent_list_removed_event_date:
                break

        if recent_list_event_date is None:
            if len(history.history) > 0:
                logger.warning(f"Warning: No listed event found in history when correcting to ListRemoved status, address: {history.address}")
                should_add_event = False
            else:
                # For empty history, add list removed event
                should_add_event = True
        elif recent_list_removed_event_date is None or recent_list_removed_event_date < recent_list_event_date:
                # No list removed event found, need to add one
                should_add_event = True

        if should_add_event:
            list_removed_date = last_updated

            # Calculate list removed date based on next event after listed event
            if recent_list_event_index > 0 and recent_list_event_index + 1 < len(history.history):
                event_after_list = history.history[recent_list_event_index+1]
                rent_event_set = { PropertyHistoryEventType.ListedForRent, PropertyHistoryEventType.RentalRemoved }
                if event_after_list.event_type in rent_event_set:
                    list_removed_date = event_after_list.datetime - timedelta(hours=12)

            # Add a delisted event
            list_removed_event = IPropertyHistoryEvent(
                id=str(uuid.uuid4()),
                datetime = list_removed_date,
                event_type = PropertyHistoryEventType.ListRemoved,
                description = "ListRemoved (added by system correction), information maybe missing",
                source = "system",
                source_id = None,
                price = None,
            )
            logger.info(f"Correcting property history by adding event: {list_removed_event}, address: {history.address}")
            history.addEvent(list_removed_event)

# TODO: handle metadata change? Like address or property type change.
def update_property_from_raw_data(
        raw_data: RawPropertyData,
        existing_property: IProperty,
        ) -> Tuple[IPropertyMetadata, IPropertyHistory]:

    logger = logger_factory.get_logger(__name__)
    # Updated time
    last_updated = parse_datetime_as_utc(raw_data.scrapedAt)

    # Parse history
    new_history = parse_property_history(
        data = raw_data,
        address = existing_property.address,
        last_updated = last_updated,
    )
    # Need to merge history first in case history is removed on closed property
    existing_property.update_history(new_history)

    # Inactive property may have many data fields omitted, only update the status
    status_raw_str = raw_data.status
    if not isinstance(status_raw_str, str):
        raise ValueError(f"Status: {status_raw_str} is not str")

    print(existing_property.history)

    new_status = parse_property_status(status_raw_str, existing_property.history)
    if new_status != existing_property.status:
        logger.info(f"Property status changed from {existing_property.status} to {new_status} for address: {existing_property.address}, id: {existing_property.id}")
        existing_property.metadata.update_status(new_status)

        # Reset property price if property is sold
        if new_status == PropertyStatus.Sold:
            existing_property.metadata.update_price(None)

        correct_property_history(
            existing_property.status,
            new_status,
            existing_property.history,
            last_updated,
        )
        logger.info(f"Updated property after correction: {existing_property.metadata}, history: {existing_property.history}")

    return existing_property.metadata, existing_property.history

# Function to parse raw data and create/update property
def parse_raw_data_to_property(
        raw_data: RawPropertyData,
        existing_property: IProperty | None = None,
        ) -> Tuple[IPropertyMetadata, IPropertyHistory]:

    # Need to use different logic for update since some fields may be missing
    if existing_property:
        return update_property_from_raw_data(raw_data, existing_property)

    # Skip ready to build properties since it misses many required fields
    if raw_data.readyToBuildTag:
        raise PropertyDataStreamParsingError(
            message = f"Property is marked as ready to build: {raw_data.address}",
            input_data=raw_data,
            error_code = PropertyDataStreamParsingErrorCode.ReadyToBuildTagEncountered,
            error_data = raw_data.address,
        )

    # Parse property type
    try:
        property_type = parse_property_type(raw_data.propertyType)
    except PropertyDataStreamParsingError as error:
        if error.error_code == PropertyDataStreamParsingErrorCode.UnknownPropertyType:
            error.input_data = raw_data
        raise error

    # Skip Vacant land since most of its properties are missing
    if property_type == PropertyType.VacantLand:
        error_msg = f"Vacant land property detected: {raw_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = raw_data,
            error_code = PropertyDataStreamParsingErrorCode.VacantLandEncountered,
            error_data = None,
        )

    validate_redfin_property_entry(raw_data)

    # Parse address
    address = IPropertyAddress(raw_data.address)

    # Parse area number and unit
    if not raw_data.area:
        error_msg = f"Area is missing for address: {raw_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data=raw_data,
            error_code = PropertyDataStreamParsingErrorCode.MissingRequiredField,
            error_data = raw_data.area,
        )
    area_parts = raw_data.area.split(" ")
    if len(area_parts) != 2:
        error_msg = f"Invalid area format: {raw_data.area} for address: {raw_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = raw_data,
            error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataFormat,
            error_data = raw_data.area,
        )
    area_number = Decimal(area_parts[0])
    if area_parts[1].lower() == "sqft":
        area_unit = AreaUnit.SquareFeet
    elif area_parts[1].lower() == "acres":
        area_unit = AreaUnit.Acres
    elif area_parts[1].lower() == "sqm2":
        area_unit = AreaUnit.SquareMeter
    else:
        error_msg = f"Unknown area unit: {area_parts[1]} for address: {raw_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = raw_data,
            error_code = PropertyDataStreamParsingErrorCode.UnknownAreaUnit,
            error_data = area_parts[1],
        )
    area = PropertyArea(area_number, area_unit)

    # Parse lot area
    lot_area = None
    if raw_data.lotArea:
        lot_area_parts = raw_data.lotArea.split(" ")
        if len(lot_area_parts) < 2:
            error_msg = f"Invalid lot area format: {raw_data.lotArea} for address: {raw_data.address}"
            raise PropertyDataStreamParsingError(
                message = error_msg,
                input_data=raw_data,
                error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataFormat,
                error_data = raw_data.lotArea,
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
            error_msg = f"Unknown lot area unit: {lot_area_parts[1]} for address: {raw_data.address}"
            raise PropertyDataStreamParsingError(
                message = error_msg,
                input_data=raw_data,
                error_code = PropertyDataStreamParsingErrorCode.UnknownAreaUnit,
                error_data = lot_area_parts[1],
            )
        lot_area = PropertyArea(lot_area_number, lot_area_unit)

    # Parse number of bedrooms and bathrooms
    number_of_bedrooms = Decimal(raw_data.numberOfBedrooms) if raw_data.numberOfBedrooms is not None else None
    number_of_bathrooms = Decimal(raw_data.numberOfBathrooms) if raw_data.numberOfBathrooms is not None else None
    year_built = raw_data.yearBuilt

    # Parse price
    price = raw_data.price

    # Create data source
    data_source = [
        IPropertyDataSource(
            source_id = raw_data.data_source_id,
            source_url = raw_data.url,
            source_name = "Redfin"
        )
    ]

    # Parse last update time
    last_updated = parse_datetime_as_utc(raw_data.scrapedAt, None)

    # Validate some logic
    if property_type != PropertyType.VacantLand and (number_of_bathrooms == None or number_of_bedrooms == None): # type: ignore[comparison-overlap]
        error_msg = f"Number of bedrooms and bathrooms must be provided for non-vacant land properties: {raw_data.address}"
        raise PropertyDataStreamParsingError(
            message = error_msg,
            input_data = raw_data,
            error_code = PropertyDataStreamParsingErrorCode.MissingRequiredField,
            error_data = {
                "propertyType": property_type,
                "numberOfBedrooms": number_of_bedrooms,
                "numberOfBathrooms": number_of_bathrooms
            },
        )

    # Parse property history
    history = parse_property_history(raw_data, address, last_updated)

    # Parse status
    status = parse_property_status(raw_data.status, history)

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
