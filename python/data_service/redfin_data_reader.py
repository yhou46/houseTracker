from typing import (
    Iterator,
    Callable,
    Any,
    Dict,
    Literal,
    Optional,
    Set,
    Tuple,
    cast,
)
from enum import Enum
import json
import uuid
from datetime import datetime, timezone, timedelta
import os
from decimal import Decimal
from zoneinfo import ZoneInfo

from crawler.redfin_spider.items import RedfinPropertyItem
from shared.iproperty import (
    PropertyArea,
    AreaUnit,
    PropertyType,
    PropertyStatus,
    IPropertyDataSource,
    IPropertyHistory,
    PropertyHistoryEventType,
    IPropertyHistoryEvent,
    IPropertyMetadata,
)
from shared.iproperty_address import IPropertyAddress, InvalidAddressError
from data_service.iproperty_data_reader import (
    IPropertyDataStream,
    PropertyDataStreamErrorHandlerType,
    IPropertyDataStreamIteratorType,
    RawPropertyData,
)
from data_service.redfin_data_parser import (
    parse_raw_data_to_property,
    PropertyDataStreamParsingError,
    PropertyDataStreamParsingErrorCode,
)

def get_raw_data_entry(json_object: Dict[str, Any]) -> RawPropertyData:
    return RawPropertyData(
        url = cast(str, json_object.get('url')),
        data_source_name = "Redfin",
        data_source_id = cast(str, json_object.get('redfinId')),
        scrapedAt = cast(str,json_object.get('scrapedAt')),
        address = cast(str,json_object.get('address')),
        area = cast(str, json_object.get('area')),
        propertyType = cast(str, json_object.get('propertyType')),
        lotArea = json_object.get('lotArea'),
        numberOfBedrooms=json_object.get('numberOfBedroom'),
        numberOfBathrooms=json_object.get('numberOfBathroom'),
        yearBuilt=json_object.get('yearBuilt', None),
        status=json_object.get('status', 'Unknown'),
        price=json_object.get('price', None),
        readyToBuildTag=json_object.get('readyToBuildTag', None),
        history = json_object.get('history', []),
    )

class RedfinFileDataReader(IPropertyDataStream):
    def __init__(self, file_path: str, error_handler: Optional[PropertyDataStreamErrorHandlerType] = None):
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

            json_object = json.loads(line)
            raw_data_entry = get_raw_data_entry(json_object)

            # Parse line into raw data entry
            return raw_data_entry

        # TODO: do we need handler here? Most errors should happen on parsing step
        except Exception as error:
            if self._error_handler:
                error_msg = f"Failed to parse line: {str(error)}"
                parsing_error = PropertyDataStreamParsingError(
                    message = error_msg,
                    input_data = line,
                    error_code = PropertyDataStreamParsingErrorCode.InvalidPropertyDataFormat,
                    error_data = line,
                )
                self._error_handler(parsing_error)
                return self.next_entry()
            raise error

    def close(self) -> None:
        if self._fileObject:
            self._fileObject.close()

if __name__ == "__main__":
    # Get the directory of the current script (data_reader.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Go up two levels to the project root, then into redfin_output
    python_project_folder = os.path.abspath(os.path.join(current_dir, ".."))
    redfin_output_path = os.path.join(
        python_project_folder,
        "crawler",
        "redfin_spider",
        "redfin_spider_monolith_output",
        "redfin_properties_20251104_183234.jsonl",
    )
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

        for raw_data_entry in reader:
            count += 1
            print(raw_data_entry)
            try:
                property_meta, property_history = parse_raw_data_to_property(raw_data_entry)
                print(f"Parsed property metadata: {property_meta}")
                print(f"Parsed property history: {property_history}")
            except PropertyDataStreamParsingError as e:
                file_error_handler(e)
            if count % 100 == 0:
                print(f"Processed {count} properties...")
                break
        print(f"Finished processing. Total properties processed: {count}, errors logged to {error_log_file}")
        reader.close()
