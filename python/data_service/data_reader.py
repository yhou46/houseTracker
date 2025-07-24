from typing import Iterator
import json
import uuid
from datetime import datetime
import os

from crawler.redfin_spider.items import RedfinPropertyItem
from shared.iproperty import IProperty, PropertyArea, AreaUnit, PropertyType, PropertyStatus, IPropertyDataSource, IPropertyHistory
from shared.iproperty_address import IPropertyAddress

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
            yearBuilt: int,
            status: str,
            price: float | None,
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

class IPropertyDataStream(Iterator[IProperty]):
    def __iter__(self) -> Iterator[IProperty]:
        self.initialize()
        return self

    def __next__(self) -> IProperty:
        entry = self.next_entry()
        if entry is None:
            self.close()
            raise StopIteration
        return entry

    def next_entry(self) -> IProperty | None:
        raise NotImplementedError("This method should be overridden by subclasses")
    
    def initialize(self) -> None:
        raise NotImplementedError("This method should be overridden by subclasses")
    
    def close(self) -> None:
        raise NotImplementedError("This method should be overridden by subclasses")

class RedfinFileDataReader(IPropertyDataStream):
    def __init__(self, file_path: str):
        self._file_path = file_path
        # self._fileObject: Any = None

    def initialize(self) -> None:
        self._fileObject = open(self._file_path, 'r')
        # Initialize any other resources

    def next_entry(self) -> IProperty | None:
        line = self._fileObject.readline()
        if not line:
            return None
        
        property_object = parse_json_str_to_property(line)
        if property_object is None:
            return self.next_entry()

        # Parse line into IProperty
        return property_object

    def close(self) -> None:
        if self._fileObject:
            self._fileObject.close()

def validate_redfin_property_entry(entry: RedfinPropertyEntry) -> None:
    if not entry.url or not isinstance(entry.url, str):
        raise ValueError(f"URL is missing or invalid: {entry.url}.")
    if not entry.redfinId or not isinstance(entry.redfinId, str):
        raise ValueError(f"Redfin ID is missing or invalid: {entry.redfinId}.")
    if not entry.address or not isinstance(entry.address, str):
        raise ValueError(f"Address is missing: {entry.address}.")
    if not entry.redfinId or not isinstance(entry.redfinId, str):
        raise ValueError(f"Redfin ID is missing: {entry.redfinId}.")
    if not entry.scrapedAt or not isinstance(entry.scrapedAt, str):
        raise ValueError(f"Scraped at timestamp is missing: {entry.scrapedAt}.")
    if entry.area and not isinstance(entry.area, str):
        raise ValueError(f"Area is missing: {entry.area}.")
    if not entry.propertyType or not isinstance(entry.propertyType, str):
        raise ValueError(f"Property type is missing: {entry.propertyType}.")
    if entry.numberOfBedrooms and not isinstance(entry.numberOfBedrooms, (int, float)):
        raise ValueError(f"Number of bedrooms must be a float: {entry.numberOfBedrooms}.")
    if entry.numberOfBathrooms and not isinstance(entry.numberOfBathrooms, (int, float)):
        raise ValueError(f"Number of bathrooms must be a float: {entry.numberOfBathrooms}.")
    if not isinstance(entry.yearBuilt, int):
        raise ValueError(f"Year built must be an integer: {entry.yearBuilt}.")
    if not isinstance(entry.status, str):
        raise ValueError(f"Status must be a string: {entry.status}.")
    if entry.price and not isinstance(entry.price, (int, float)):
        raise ValueError(f"Price must be a number: {entry.price}.")

def parse_json_str_to_property(line: str) -> IProperty | None:
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
        yearBuilt=data.get('yearBuilt', 0),
        status=data.get('status', 'Unknown'),
        price=data.get('price', None)
    )

    id = str(uuid.uuid4())

    try:

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
        else:
            raise ValueError(f"Unknown property type: {redfin_data.propertyType}")
        
        if property_type == PropertyType.VacantLand:
            print(f"Vacant land property detected: {redfin_data.address}")
            return None

        validate_redfin_property_entry(redfin_data)
        address = redfin_data.address
        
        # Parse area number and unit
        area_parts = redfin_data.area.split(" ")
        if len(area_parts) != 2:
            raise ValueError(f"Invalid area format: {redfin_data.area}")
        area_number = float(area_parts[0])
        if area_parts[1].lower() == "sqft":
            area_unit = AreaUnit.SquareFeet
        elif area_parts[1].lower() == "acres":
            area_unit = AreaUnit.Acres
        elif area_parts[1].lower() == "sqm2":
            area_unit = AreaUnit.SquareMeter
        else:
            raise ValueError(f"Unknown area unit: {area_parts[1]}")
        area = PropertyArea(area_number, area_unit)
        
        # Parse lot area
        lot_area = None
        if redfin_data.lotArea:
            lot_area_parts = redfin_data.lotArea.split(" ")
            if len(lot_area_parts) != 2:
                raise ValueError(f"Invalid lot area format: {redfin_data.lotArea}")
            lot_area_number = float(lot_area_parts[0])
            if lot_area_parts[1].lower() == "sqft":
                lot_area_unit = AreaUnit.SquareFeet
            elif lot_area_parts[1].lower() == "acres":
                lot_area_unit = AreaUnit.Acres
            elif lot_area_parts[1].lower() == "sqm2":
                lot_area_unit = AreaUnit.SquareMeter
            else:
                raise ValueError(f"Unknown lot area unit: {lot_area_parts[1]}")
            lot_area = PropertyArea(lot_area_number, lot_area_unit)
        
        # Parse number of bedrooms and bathrooms
        number_of_bedrooms = float(redfin_data.numberOfBedrooms) if redfin_data.numberOfBedrooms is not None else None
        number_of_bathrooms = float(redfin_data.numberOfBathrooms) if redfin_data.numberOfBathrooms is not None else None
        year_built = redfin_data.yearBuilt

        # Parse status
        if redfin_data.status == "Active":
            status = PropertyStatus.Active
        elif redfin_data.status == "Pending":
            status = PropertyStatus.Pending
        elif redfin_data.status == "Sold":
            status = PropertyStatus.Sold
        else:
            raise ValueError(f"Unknown property status: {redfin_data.status}")

        # Parse price
        price = redfin_data.price

        # Create data source
        data_source = [
            IPropertyDataSource(
                sourceId = redfin_data.redfinId,
                sourceUrl = redfin_data.url,
                sourceName = "Redfin"
            )
        ]

        # Parse last update time
        last_updated = datetime.fromisoformat(redfin_data.scrapedAt)

        # Parse property history
        history = IPropertyHistory(id, IPropertyAddress(address), [])

        # Validate some logic
        if property_type != PropertyType.VacantLand and (number_of_bathrooms == None or number_of_bedrooms == None):
            raise ValueError("Number of bedrooms and bathrooms must be provided for non-vacant land properties.")

        # Create property object
        property = IProperty(
            id = id,
            address = address,
            area = area,
            propertyType = property_type,
            lotArea = lot_area,
            numberOfBedrooms = number_of_bedrooms,
            numberOfBathrooms = number_of_bathrooms,
            yearBuilt = year_built,
            status = status,
            price = price,
            history = history,
            dataSource = data_source,
            lastUpdated = last_updated
        )
        return property

    except ValueError as e:
        raise ValueError(f"Failed to parse property: {line}, error: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while parsing property: {line}, error: {str(e)}")

if __name__ == "__main__":
    # Get the directory of the current script (data_reader.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Go up two levels to the project root, then into redfin_output
    python_project_folder = os.path.abspath(os.path.join(current_dir, ".."))
    redfin_output_path = os.path.join(python_project_folder, "crawler", "redfin_output", "redfin_properties_20250722_183208.jsonl")
    print(redfin_output_path)

    reader = RedfinFileDataReader(redfin_output_path)
    count = 0
    for property in reader:
        count += 1
        print(f"{property}\nCount: {count}\n")
