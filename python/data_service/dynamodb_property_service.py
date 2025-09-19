from typing import (
    List,
    Literal,
    Dict,
    Any,
    Tuple,
    Mapping,
    cast,
)
from datetime import datetime
from enum import Enum
import os
from decimal import Decimal
import time

import boto3
from boto3.dynamodb.conditions import Key
from mypy_boto3_dynamodb.type_defs import (
    AttributeDefinitionTypeDef,
    KeySchemaElementTypeDef,
    GlobalSecondaryIndexTypeDef,
    GlobalSecondaryIndexOutputTypeDef,
    TableAttributeValueTypeDef,
)
from mypy_boto3_dynamodb.client import DynamoDBClient
from botocore.exceptions import ClientError

from shared.iproperty import (
    IProperty,
    PropertyType,
    IPropertyHistory,
    IPropertyHistoryEvent,
    PropertyArea,
    AreaUnit,
    PropertyStatus,
    IPropertyDataSource,
    IPropertyMetadata,
    PropertyHistoryEventType,
)
from shared.iproperty_address import IPropertyAddress
import shared.logger_factory as logger_factory
from data_service.redfin_data_reader import (
    RedfinFileDataReader,
    PropertyDataStreamParsingError,
    IPropertyDataStream,
)
from data_service.iproperty_service import (
    IPropertyService,
    PropertyQueryPattern,
    IPropertyServiceLastEvaluateKeyType,
)

class DynamoDbPropertyTableEntityType(Enum):
    Property = "PROPERTY"
    PropertyHistory = "HISTORY"

class DynamoDbPropertyTableAttributeName(Enum):
    # Required attributes for keys and indexes
    PK = "PK"  # Partition Key
    SK = "SK"  # Sort Key
    Status = "Status"
    AddressPropertyTypeIndex = "AddressPropertyTypeIndex"
    AddressHash = "AddressHash"

    # Other attributes
    Id = "Id"

    # Address related attributes
    Address = "Address"
    Address_StreetName = "StreetName"
    Address_Unit = "Unit"
    Address_City = "City"
    Address_State = "State"
    Address_ZipCode = "ZipCode"

    # Property area related attributes
    Area = "Area"
    Area_Value = "Value"
    Area_Unit = "Unit"

    # Property lot area related attributes
    LotArea = "LotArea"
    LotArea_Value = "Value"
    LotArea_Unit = "Unit"

    # Property type
    PropertyType = "PropertyType"

    # Number of bedrooms and bathrooms
    NumberOfBedrooms = "NumberOfBedrooms"
    NumberOfBathrooms = "NumberOfBathrooms"

    # Year built
    YearBuilt = "YearBuilt"

    # Price
    Price = "Price"

    # Last updated
    LastUpdated = "LastUpdated"

    # Data sources
    DataSources = "DataSources"
    DataSource_SourceId = "SourceId"
    DataSource_SourceUrl = "SourceUrl"
    DataSource_SourceName = "SourceName"

    # Property history event related attributes
    HistoryEventType = "EventType"
    HistoryEventDescription = "Description"
    HistoryEventPrice = "Price"
    HistoryEventSource = "Source"
    HistoryEventSourceId = "SourceId"
    HistoryEventDatetime = "Datetime"

class DynamoDbPropertyTableGlobalSecondaryIndexName(Enum):
    StatusAddressPropertyTypeIndex = "StatusAddressPropertyTypeIndex"
    AddressHashIndex = "AddressHashIndex"

def get_pk_from_entity(entity_id: str, entity_type: DynamoDbPropertyTableEntityType) -> str:
    return f"{entity_type.value}#{entity_id}"

# TODO: update time should be created as a GSI attribute for query purpose
def get_sk_from_entity(
        entity_id: str,
        entity_type: DynamoDbPropertyTableEntityType,
        time_in_utc: datetime | None,
        ) -> str:
    return f"{entity_type.value}#{entity_id}#{time_in_utc.isoformat()}" if time_in_utc != None else f"{entity_type.value}#{entity_id}"

def get_address_property_type_index(state: str, zip_code: str, city: str, property_type: PropertyType) -> str:
    return f"{state}#{city}#{zip_code}#{property_type.value}"

def _parse_address_property_type_index(index_value: str) -> Dict[str, str | int]:
    parts = index_value.split("#")
    if len(parts) != 4:
        raise ValueError(f"Invalid index value: {index_value}")
    parsed_result: Dict[str, str | int] = {
        "state": parts[0],
        "city": parts[1],
        "zip_code": int(parts[2]),
        "property_type": parts[3]
    }

    if parsed_result.get("property_type") not in PropertyType:
        raise ValueError(f"Invalid property type: {parsed_result.get('property_type')}")

    return parsed_result

def get_property_id_from_pk(pk: str) -> str:
    """
    Extracts the property ID from the partition key (PK) format.

    Args:
        pk (str): The partition key in the format "PROPERTY#<property_id>".

    Returns:
        str: The extracted property ID.
    """
    parts = pk.split("#")
    if len(parts) != 2 or parts[0] != DynamoDbPropertyTableEntityType.Property.value:
        raise ValueError(f"Invalid PK format: {pk}")
    return parts[1]

def get_history_event_id_from_sk(sk: str) -> str | None:
    """
    Extracts the history event ID from the sort key (SK) format.

    Args:
        sk (str): The sort key in the format "HISTORY#<event_id>#<datetime>".

    Returns:
        str: The extracted history event ID.
    """
    parts = sk.split("#")
    if len(parts) < 3 or parts[0] != DynamoDbPropertyTableEntityType.PropertyHistory.value:
        print(f"Invalid SK format: {sk} or it is not a history event")
        return None
    return parts[1]

def convert_property_history_event_to_dynamodb_item(
        property_id: str,
        history_event: IPropertyHistoryEvent,
        ) -> Dict[str, Any]:
    item: Dict[str, Any] = dict()

    # Set up partition key and sort key


    item[DynamoDbPropertyTableAttributeName.PK.value] = get_pk_from_entity(property_id, DynamoDbPropertyTableEntityType.Property)
    item[DynamoDbPropertyTableAttributeName.SK.value] = get_sk_from_entity(history_event.id, DynamoDbPropertyTableEntityType.PropertyHistory, history_event.datetime)

    # Set up global secondary indexes, no GIS for history events

    # Set up other attributes
    item[DynamoDbPropertyTableAttributeName.HistoryEventType.value] = history_event.event_type.value
    item[DynamoDbPropertyTableAttributeName.HistoryEventDescription.value] = history_event.description
    item[DynamoDbPropertyTableAttributeName.HistoryEventPrice.value] = Decimal(history_event.price) if history_event.price is not None else None
    item[DynamoDbPropertyTableAttributeName.HistoryEventSource.value] = history_event.source
    item[DynamoDbPropertyTableAttributeName.HistoryEventSourceId.value] = history_event.source_id
    item[DynamoDbPropertyTableAttributeName.HistoryEventDatetime.value] = history_event.datetime.isoformat()
    return item

def convert_property_history_to_dynamodb_item(property_id: str, history: IPropertyHistory) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    # Set up partition key and sort key
    for event in history.history:
        item = convert_property_history_event_to_dynamodb_item(property_id, event)
        items.append(item)
    return items

def convert_property_metadata_to_dynamodb_items(metadata: IPropertyMetadata, property_id: str) -> Dict[str, Any]:
    metadata_item: Dict[str, Any] = dict()

    # Set up partition key and sort key
    metadata_item[DynamoDbPropertyTableAttributeName.PK.value] = get_pk_from_entity(property_id, DynamoDbPropertyTableEntityType.Property)
    metadata_item[DynamoDbPropertyTableAttributeName.SK.value] = get_sk_from_entity(property_id, DynamoDbPropertyTableEntityType.Property, None)

    # Set up global secondary indexes
    # Check table creation for attribute details
    metadata_item[DynamoDbPropertyTableAttributeName.AddressPropertyTypeIndex.value] = get_address_property_type_index(metadata.address.state, metadata.address.zip_code, metadata.address.city, metadata.property_type)
    metadata_item[DynamoDbPropertyTableAttributeName.AddressHash.value] = metadata.address.address_hash
    metadata_item[DynamoDbPropertyTableAttributeName.Status.value] = metadata.status.value

    # Other property entities
    metadata_item[DynamoDbPropertyTableAttributeName.Id.value] = property_id
    metadata_item[DynamoDbPropertyTableAttributeName.Address.value] = {
        DynamoDbPropertyTableAttributeName.Address_StreetName.value: metadata.address.street_name,
        DynamoDbPropertyTableAttributeName.Address_Unit.value: metadata.address.unit,
        DynamoDbPropertyTableAttributeName.Address_City.value: metadata.address.city,
        DynamoDbPropertyTableAttributeName.Address_State.value: metadata.address.state,
        DynamoDbPropertyTableAttributeName.Address_ZipCode.value: metadata.address.zip_code
    }
    metadata_item[DynamoDbPropertyTableAttributeName.Area.value] = {
        DynamoDbPropertyTableAttributeName.Area_Value.value: Decimal(metadata.area.value),
        DynamoDbPropertyTableAttributeName.Area_Unit.value: metadata.area.unit.value
    } if metadata.area else None
    metadata_item[DynamoDbPropertyTableAttributeName.PropertyType.value] = metadata.property_type.value
    metadata_item[DynamoDbPropertyTableAttributeName.LotArea.value] = {
        DynamoDbPropertyTableAttributeName.LotArea_Value.value: Decimal(metadata.lot_area.value),
        DynamoDbPropertyTableAttributeName.LotArea_Unit.value: metadata.lot_area.unit.value
    } if metadata.lot_area else None
    metadata_item[DynamoDbPropertyTableAttributeName.NumberOfBedrooms.value] = Decimal(metadata.number_of_bedrooms) if metadata.number_of_bedrooms is not None else None
    metadata_item[DynamoDbPropertyTableAttributeName.NumberOfBathrooms.value] = Decimal(metadata.number_of_bathrooms) if metadata.number_of_bathrooms is not None else None
    metadata_item[DynamoDbPropertyTableAttributeName.YearBuilt.value] = metadata.year_built
    metadata_item[DynamoDbPropertyTableAttributeName.Price.value] = Decimal(metadata.price) if metadata.price is not None else None
    metadata_item[DynamoDbPropertyTableAttributeName.LastUpdated.value] = metadata.last_updated.isoformat()
    metadata_item[DynamoDbPropertyTableAttributeName.DataSources.value] = [
        {
            DynamoDbPropertyTableAttributeName.DataSource_SourceId.value: ds.source_id,
            DynamoDbPropertyTableAttributeName.DataSource_SourceUrl.value: ds.source_url,
            DynamoDbPropertyTableAttributeName.DataSource_SourceName.value: ds.source_name
        } for ds in metadata.data_sources
    ]
    return metadata_item

def convert_property_to_dynamodb_items(property: IProperty) -> List[Dict[str, Any]]:
    """
    Convert IProperty object to a DynamoDB item format.
    NOTE: DynamoDB does not support float but only Decimal.
    """

    property_item = convert_property_metadata_to_dynamodb_items(property.metadata, property.id)
    # Convert history
    history_items = convert_property_history_to_dynamodb_item(property.id, property._history)

    # Combine property item with history items
    property_items = [property_item] + history_items

    return property_items

def convert_dynamodb_item_to_property(items: List[Dict[str, Any]]) -> IProperty:
    """
    Convert DynamoDB items back to IProperty object.

    Args:
        items: List of DynamoDB items representing a property and its history

    Returns:
        IProperty: The reconstructed property object
    """
    if not items:
        raise ValueError("No items provided to convert")

    # Separate property item from history items
    property_item = None
    history_items = []

    for item in items:
        pk = item.get(DynamoDbPropertyTableAttributeName.PK.value, "")
        sk = item.get(DynamoDbPropertyTableAttributeName.SK.value, "")

        # Check if this is a property item or history item
        if sk.startswith(f"{DynamoDbPropertyTableEntityType.Property.value}#"):
            property_item = item
        elif sk.startswith(f"{DynamoDbPropertyTableEntityType.PropertyHistory.value}#"):
            history_items.append(item)

    if not property_item:
        raise ValueError("No property item found in the provided items")

    # Extract property ID from PK
    property_id = str(property_item[DynamoDbPropertyTableAttributeName.Id.value])

    # Extract address information
    address_data = property_item[DynamoDbPropertyTableAttributeName.Address.value]
    address_str = f"{address_data[DynamoDbPropertyTableAttributeName.Address_StreetName.value]}"
    if address_data.get(DynamoDbPropertyTableAttributeName.Address_Unit.value):
        address_str += f" {address_data[DynamoDbPropertyTableAttributeName.Address_Unit.value]}"
    address_str += f", {address_data[DynamoDbPropertyTableAttributeName.Address_City.value]}, {address_data[DynamoDbPropertyTableAttributeName.Address_State.value]} {address_data[DynamoDbPropertyTableAttributeName.Address_ZipCode.value]}"

    address = IPropertyAddress(address_str)

    # Extract area information
    area_data = property_item.get(DynamoDbPropertyTableAttributeName.Area.value)
    if not area_data:
        raise ValueError("Area information is required but not found in DynamoDB item")
    area_value = Decimal(area_data[DynamoDbPropertyTableAttributeName.Area_Value.value])
    area_unit = AreaUnit(area_data[DynamoDbPropertyTableAttributeName.Area_Unit.value])
    area = PropertyArea(area_value, area_unit)

    # Extract property type
    property_type = PropertyType(property_item[DynamoDbPropertyTableAttributeName.PropertyType.value])

    # Extract lot area information
    lot_area_data = property_item.get(DynamoDbPropertyTableAttributeName.LotArea.value)
    lot_area = None
    if lot_area_data:
        lot_area_value = Decimal(lot_area_data[DynamoDbPropertyTableAttributeName.LotArea_Value.value])
        lot_area_unit = AreaUnit(lot_area_data[DynamoDbPropertyTableAttributeName.LotArea_Unit.value])
        lot_area = PropertyArea(lot_area_value, lot_area_unit)

    # Extract bedrooms and bathrooms
    number_of_bedrooms = Decimal(property_item[DynamoDbPropertyTableAttributeName.NumberOfBedrooms.value]) if property_item.get(DynamoDbPropertyTableAttributeName.NumberOfBedrooms.value) is not None else Decimal(0)
    number_of_bathrooms = Decimal(property_item[DynamoDbPropertyTableAttributeName.NumberOfBathrooms.value]) if property_item.get(DynamoDbPropertyTableAttributeName.NumberOfBathrooms.value) is not None else Decimal(0)

    # Extract year built
    year_built = property_item.get(DynamoDbPropertyTableAttributeName.YearBuilt.value)

    # Extract status
    status = PropertyStatus(property_item[DynamoDbPropertyTableAttributeName.Status.value])

    # Extract price
    price = Decimal(property_item[DynamoDbPropertyTableAttributeName.Price.value]) if property_item.get(DynamoDbPropertyTableAttributeName.Price.value) is not None else None

    # Extract last updated
    last_updated = datetime.fromisoformat(property_item[DynamoDbPropertyTableAttributeName.LastUpdated.value])

    # Extract data sources
    data_sources_data = property_item.get(DynamoDbPropertyTableAttributeName.DataSources.value, [])
    data_sources = []
    for ds_data in data_sources_data:
        data_source = IPropertyDataSource(
            source_id=ds_data[DynamoDbPropertyTableAttributeName.DataSource_SourceId.value],
            source_url=ds_data[DynamoDbPropertyTableAttributeName.DataSource_SourceUrl.value],
            source_name=ds_data[DynamoDbPropertyTableAttributeName.DataSource_SourceName.value]
        )
        data_sources.append(data_source)

    # Create property metadata
    property_metadata = IPropertyMetadata(
        address=address,
        area=area,
        property_type=property_type,
        lot_area=lot_area,
        number_of_bedrooms=number_of_bedrooms,
        number_of_bathrooms=number_of_bathrooms,
        year_built=year_built,
        status=status,
        price=price,
        last_updated=last_updated,
        data_sources=data_sources
    )

    # Extract history events
    history_events = []
    for history_item in history_items:
        # Extract event ID from SK
        sk_parts = history_item[DynamoDbPropertyTableAttributeName.SK.value].split("#")
        event_id = sk_parts[1] if len(sk_parts) > 1 else ""

        # Extract event datetime from SK
        event_datetime_str = sk_parts[2] if len(sk_parts) > 2 else ""
        event_datetime = datetime.fromisoformat(event_datetime_str)

        # Extract other event properties
        event_type = PropertyHistoryEventType(history_item[DynamoDbPropertyTableAttributeName.HistoryEventType.value])
        description = history_item[DynamoDbPropertyTableAttributeName.HistoryEventDescription.value]
        event_price = Decimal(history_item[DynamoDbPropertyTableAttributeName.HistoryEventPrice.value]) if history_item.get(DynamoDbPropertyTableAttributeName.HistoryEventPrice.value) is not None else None
        source = history_item.get(DynamoDbPropertyTableAttributeName.HistoryEventSource.value)
        source_id = history_item.get(DynamoDbPropertyTableAttributeName.HistoryEventSourceId.value)

        history_event = IPropertyHistoryEvent(
            id=event_id,
            datetime=event_datetime,
            event_type=event_type,
            description=description,
            source=source,
            source_id=source_id,
            price=event_price
        )
        history_events.append(history_event)

    # Create property history
    property_history = IPropertyHistory(
        address=address,
        history=history_events,
        last_updated=last_updated,
    )

    # Create and return the IProperty object
    return IProperty(
        id=property_id,
        property_metadata=property_metadata,
        property_history=property_history
    )

def check_dynamodb_table_exists(table_name: str, dynamodb_client: DynamoDBClient) -> bool:
    """
    Checks if a DynamoDB table exists in the current AWS region.

    Args:
        table_name (str): The name of the DynamoDB table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        existing_tables = dynamodb_client.list_tables()['TableNames']
        print(f"Existing tables: {existing_tables}")
        return table_name in existing_tables
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def create_dynambodb_table_for_property(
        table_name: str,
        region_name: str,
        billing_mode: Literal['PAY_PER_REQUEST', 'PROVISIONED'],
        ) -> None:
    dynamodb_resource = boto3.resource('dynamodb', region_name=region_name)
    dynamodb_client = boto3.client('dynamodb', region_name=region_name)

    # Check if table already exists
    if check_dynamodb_table_exists(table_name, dynamodb_client):
        print(f"Table {table_name} already exists.")
        return

    # Define key schema
    key_schema: List[KeySchemaElementTypeDef] = [
        {"AttributeName": DynamoDbPropertyTableAttributeName.PK.value, "KeyType": "HASH"},
        {"AttributeName": DynamoDbPropertyTableAttributeName.SK.value, "KeyType": "RANGE"},
    ]

    # Define Global Secondary Indexes
    global_secondary_indexes: List[GlobalSecondaryIndexTypeDef | GlobalSecondaryIndexOutputTypeDef] = [
        {
            "IndexName": DynamoDbPropertyTableGlobalSecondaryIndexName.StatusAddressPropertyTypeIndex.value,
            "KeySchema": [
                {"AttributeName": DynamoDbPropertyTableAttributeName.Status.value, "KeyType": "HASH"},
                {"AttributeName": DynamoDbPropertyTableAttributeName.AddressPropertyTypeIndex.value, "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        },
        {
            "IndexName": DynamoDbPropertyTableGlobalSecondaryIndexName.AddressHashIndex.value,
            "KeySchema": [
                {"AttributeName": DynamoDbPropertyTableAttributeName.AddressHash.value, "KeyType": "HASH"},
            ],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        }
    ]

    attribute_definitions: List[AttributeDefinitionTypeDef] = [
        {
            "AttributeName": DynamoDbPropertyTableAttributeName.PK.value,
            "AttributeType": "S",
        }, # EntityType#EntityId
        {
            "AttributeName": DynamoDbPropertyTableAttributeName.SK.value,
            "AttributeType": "S",
        }, # EntityType#EntityId#TimeInUtc
        {
            "AttributeName": DynamoDbPropertyTableAttributeName.Status.value,
            "AttributeType": "S",
        },
        {
            "AttributeName": DynamoDbPropertyTableAttributeName.AddressPropertyTypeIndex.value,
            "AttributeType": "S",
        }, # State#City#Zip#PropertyType
        {
            "AttributeName": DynamoDbPropertyTableAttributeName.AddressHash.value,
            "AttributeType": "S",
        },
    ]

    # Create table
    table = dynamodb_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        GlobalSecondaryIndexes=global_secondary_indexes,
        BillingMode=billing_mode,
        DeletionProtectionEnabled=True,
        )
    table.wait_until_exists()

    print(f"Table {table_name} created successfully")

type DynamoDBPropertyServiceLastEvaluatedKeyType = Mapping[str, str]

class DynamoDBPropertyService(IPropertyService):
    def __init__(self, table_name: str, region_name: str = "us-west-2"):
        """
        Initialize DynamoDB service

        Args:
            table_name: DynamoDB table name (defaults to schema default)
            region_name: AWS region name
        """
        # Set up logging
        self.logger = logger_factory.get_logger(f"{__name__}.{self.__class__.__name__}")

        # Check if table exists
        self.table_name = table_name
        self.dynamodb_client = boto3.client('dynamodb', region_name=region_name)
        if not check_dynamodb_table_exists(table_name, self.dynamodb_client):
            raise ValueError(f"DynamoDB table: {table_name} does not exist in region {region_name}.")
        self.dynamodb_resource = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb_resource.Table(self.table_name)
        self._db_query_result_limit = 500
        self._query_return_limit = 1000

    """
    ===========================================
    Public methods
    ===========================================
    """
    def get_property_by_id(self, property_id: str) -> IProperty | None:
        """
        Retrieve a property by its ID from the DynamoDB table.

        Args:
            property_id (str): The ID of the property to retrieve.

        Returns:
            Optional[IProperty]: The property object if found, otherwise None.
        """
        try:
            partition_key = get_pk_from_entity(property_id, DynamoDbPropertyTableEntityType.Property)
            response = self.table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('PK').eq(partition_key))
            items = response['Items']

            if not items:
                return None

            # Convert DynamoDB items to IProperty object
            return convert_dynamodb_item_to_property(items)
        except ClientError as error:
            self.logger.error(f"Error retrieving property with ID {property_id}: {error.response['Error']['Message']}")
            raise error

    def get_property_by_address(self, address: IPropertyAddress) -> IProperty | None:
        """
        Retrieve a property by its address from the DynamoDB table.

        Args:
            address (IPropertyAddress): The address of the property to retrieve.

        Returns:
            Optional[IProperty]: The property object if found, otherwise None.
        """
        try:
            response = self.table.query(
                IndexName="AddressHashIndex",
                KeyConditionExpression=boto3.dynamodb.conditions.Key("AddressHash").eq(address.address_hash),
            )
            items = response['Items']

            self.logger.info(f"Get items from DB by address {address.address_hash}: {items}")

            if not items:
                self.logger.info(f"No property found with address {address.address_hash}")
                return None

            # Get the property ID from the first item (all items should have the same property ID)
            PK = items[0].get(DynamoDbPropertyTableAttributeName.PK.value)
            if not PK:
                self.logger.error(f"No property ID found in items for address {address.address_hash}")
                raise ValueError(f"No property ID found in items for address {address.address_hash}")
            property_id = get_property_id_from_pk(str(PK))
            if not property_id:
                self.logger.warning(f"Failed to extract property ID from PK {str(PK)} for address {address.address_hash}")
                return None

            # Query the full property using the ID
            return self.get_property_by_id(property_id)
        except ClientError as error:
            self.logger.error(f"Error retrieving property with address {address}: {error.response['Error']['Message']}")
            raise error

    def create_or_update_property(self, property_metadata: IPropertyMetadata, property_history: IPropertyHistory) -> IProperty:
        """
        Create or update a property in the DynamoDB table.

        Args:
            property (IProperty): The property object to save.

        Returns:
            None
        """
        # Check if the property already exists
        existing_property = self.get_property_by_address(property_metadata.address)
        new_property = None
        if existing_property:
            self.logger.info(f"Property with ID {existing_property.id} already exists. Updating the property.")

            # Update DB record
            self._update_property_metadata(
                existing_metadata=existing_property.metadata,
                new_metadata=property_metadata,
                property_id=existing_property.id,
            )
            self._update_property_history(
                existing_history=existing_property.history,
                new_history=property_history,
                property_id=existing_property.id,
            )

            # Merge the existing property with the new one
            # self.logger.info(f"Existing property info before update:\n{existing_property}\n")
            existing_property.update_metadata(property_metadata)
            existing_property.update_history(property_history)
            # self.logger.info(f"Existing property info after update:\n{existing_property}\n")

            # Debug
            # new_property = self.get_property_by_id(existing_property.id)
            # if new_property == None:
            #     raise ValueError(f"new property should not be none, query id: {existing_property.id}")
            # if new_property != existing_property:
            #     IProperty.compare_print_diff(new_property, existing_property)
            #     self.logger.error(f"db record is not same as record in memory after DB update for property: id={new_property.id}, address={new_property.metadata.address}")
            new_property = existing_property

        else:
            self.logger.info(f"Property does not exist in DB. Will create new record")
            new_property = IProperty(
                IProperty.generate_id(),
                property_metadata,
                property_history,
            )
            self.logger.info(f"Generating property id: {new_property.id}")
            self.logger.info(f"New property info:\n{new_property}\n")

            self.logger.info(f"Saving property with ID {new_property.id}, address hash: {new_property.address.address_hash} to DynamoDB.")
            self._write_property(new_property)
        return new_property

    def delete_property_by_id(self, property_id: str) -> None:
        """
        Delete a property by its ID from the DynamoDB table.

        Args:
            property_id (str): The ID of the property to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        try:
            self.logger.info(f"Will delete property with id: {property_id}")
            property_items = self._query_items_by_property_id(property_id)

            with self.table.batch_writer() as writer:
                for item in property_items:
                    writer.delete_item(Key={
                        'PK': item[DynamoDbPropertyTableAttributeName.PK.value],
                        'SK': item[DynamoDbPropertyTableAttributeName.SK.value],
                    })
        except ClientError as error:
            self.logger.error(f"Error deleting property with ID {property_id}: {error.response['Error']['Message']}")
            raise error

    def query_properties(
        self,
        query: PropertyQueryPattern,
        limit: int,
        exclusive_start_key: DynamoDBPropertyServiceLastEvaluatedKeyType | None = None,
        ) -> Tuple[List[IProperty], DynamoDBPropertyServiceLastEvaluatedKeyType | None]:

        # if not query.status:
        #     raise NotImplementedError("Method not implemented yet")

        if query.state != "WA":
            raise ValueError(f"Invalid state: {query.state}. States other than WA is not supported")

        raise NotImplementedError("Method not implemented yet")

    def close(self) -> None:
        if self.dynamodb_client:
            self.dynamodb_client.close()

    """
    ===========================================
    Private methods
    ===========================================
    """
    def _query_items_by_property_id(self, property_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve a property by its ID from the DynamoDB table.

        Args:
            property_id (str): The ID of the property to retrieve.

        Returns:
            Optional[IProperty]: The property object if found, otherwise None.
        """
        self.logger.info(f"Querying property with ID {property_id} from DynamoDB table {self.table_name}")
        try:
            partition_key = get_pk_from_entity(property_id, DynamoDbPropertyTableEntityType.Property)
            response = self.table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('PK').eq(partition_key))
            items = response['Items']

            # for item in items:
            #     self.logger.info(item)
            return items if items else []
        except ClientError as error:
            self.logger.error(f"Error retrieving property with ID {property_id}: {error.response['Error']['Message']}")
            raise error

    def _write_items(self, items: List[Dict[str, Any]]) -> None:
        if len(items) == 0:
            self.logger.info("No items to write to DynamoDB.")
            return
        try:
            with self.table.batch_writer() as writer:
                for item in items:
                    writer.put_item(Item=item)
            self.logger.info(f"Successfully wrote {len(items)} items to DynamoDB table {self.table.name}.")
        except ClientError as err:
            self.logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise err

    def _write_property(self, property: IProperty) -> None:
        """
        Write property data to DynamoDB table.
        It will overwrite any existing data for the property.
        """
        items = convert_property_to_dynamodb_items(property)
        self.logger.info(f"Number of items to save: {len(items)}")
        self._write_items(items)

    def _update_property_metadata(
            self,
            existing_metadata: IPropertyMetadata,
            new_metadata: IPropertyMetadata,
            property_id: str,
            ) -> None:
        """
        Update property metadata
        """
        if (existing_metadata == new_metadata):
            self.logger.info("metadata is exactly the same, skip the update")
            return

        if (existing_metadata.last_updated >= new_metadata.last_updated):
            self.logger.info(f"existing metadata last updated {existing_metadata.last_updated} is newer than or same as new metadata last updated {new_metadata.last_updated}, skip the update")
            return

        items_to_be_updated = convert_property_metadata_to_dynamodb_items(new_metadata, property_id)

        try:
            with self.table.batch_writer() as writer:
                # Overwrite with new metadata
                writer.put_item(items_to_be_updated)

        except ClientError as err:
            self.logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise err

    def _update_property_history(
            self,
            existing_history: IPropertyHistory,
            new_history: IPropertyHistory,
            property_id: str,
            ) -> None:

        new_items = []
        for event in new_history.history:
            if event not in existing_history.history:
                new_items.append(convert_property_history_event_to_dynamodb_item(property_id, event))
        self._write_items(new_items)

    @staticmethod
    def _get_index_for_query(query: PropertyQueryPattern) -> DynamoDbPropertyTableGlobalSecondaryIndexName:
        return DynamoDbPropertyTableGlobalSecondaryIndexName.StatusAddressPropertyTypeIndex

    def _query_properties(
        self,
        status: PropertyStatus,
        query: PropertyQueryPattern,
        limit: int | None = None,
        exclusive_start_key: DynamoDBPropertyServiceLastEvaluatedKeyType | None = None,
        ) -> Tuple[List[IProperty], DynamoDBPropertyServiceLastEvaluatedKeyType | None]:
        """
        It is highly dependent on the Global Secondary Index created for the table
        """

        gsi_index = DynamoDBPropertyService._get_index_for_query(query)
        self.logger.info(f"GSI used for query: {gsi_index.value}")
        query_limit: int = limit if limit else self._query_return_limit

        # Get sort key used for query
        sort_key = f"{query.state}#"
        if query.city_list and len(query.city_list) == 1:
            sort_key += f"{query.city_list[0]}#"
            if query.zip_code_list and len(query.zip_code_list) == 1:
                sort_key += f"{query.zip_code_list[0]}#"
                if query.property_type_list and len(query.property_type_list) == 1:
                    sort_key += f"{query.property_type_list[0].value}"
        self.logger.info(f"Sort key for query: {sort_key}")

        last_evaluated_key: Mapping[str, TableAttributeValueTypeDef] | None = exclusive_start_key
        result_property_id_list = []
        while True:
            if last_evaluated_key:
                response = self.table.query(
                    IndexName = gsi_index.value,
                    KeyConditionExpression =
                        Key(DynamoDbPropertyTableAttributeName.Status.value).eq(status.value) & \
                        Key(DynamoDbPropertyTableAttributeName.AddressPropertyTypeIndex.value).begins_with(sort_key),
                    Limit = self._db_query_result_limit,
                    ExclusiveStartKey=last_evaluated_key,
                )
            else:
                response = self.table.query(
                    IndexName = gsi_index.value,
                    KeyConditionExpression =
                        Key(DynamoDbPropertyTableAttributeName.Status.value).eq(status.value) & \
                        Key(DynamoDbPropertyTableAttributeName.AddressPropertyTypeIndex.value).begins_with(sort_key),
                    Limit = self._db_query_result_limit,
                )

            items = response.get("Items")
            if not isinstance(items, list):
                raise ValueError(f"returned items is not List type")

            # Filter out items that do not match the query
            filtered_items = []
            filtered_property_ids: List[str] = []
            for item in items:
                item_sk_value = item.get(DynamoDbPropertyTableAttributeName.AddressPropertyTypeIndex.value)
                if not isinstance(item_sk_value, str):
                    raise ValueError(f"sort key is not a string: {str(item_sk_value)}")
                parsed_sk = _parse_address_property_type_index(item_sk_value)
                if parsed_sk.get("state") != query.state:
                    continue
                if query.city_list and parsed_sk.get("city") not in query.city_list:
                    continue
                if query.zip_code_list and parsed_sk.get("zip_code") not in query.zip_code_list:
                    continue
                if query.property_type_list:
                    target_property_type_str_list = [ property_type.value for property_type in query.property_type_list]
                    if parsed_sk.get("property_type") not in target_property_type_str_list:
                        continue

                item_pk_value = item.get(DynamoDbPropertyTableAttributeName.PK.value)
                if item_sk_value and isinstance(item_pk_value, str):
                    item_property_id = get_property_id_from_pk(item_pk_value)
                    filtered_property_ids.append(item_property_id)
                else:
                    raise ValueError(f"partition key is not a string: {str(item_pk_value)}")

                filtered_items.append(item)


            last_evaluated_key = response.get("LastEvaluatedKey")
            self.logger.info(f"last evaludated key: {last_evaluated_key}")
            result_property_id_list.extend(filtered_property_ids)

            if not last_evaluated_key:
                break
            if len(result_property_id_list) > query_limit:
                self.logger.info(f"Quit earlier since limit exceeded, last evalulated key: f{last_evaluated_key}")
                break

        self.logger.info(
            f"query result count: {len(result_property_id_list)}"
        )
        self.logger.info(
            f"query result: {result_property_id_list}"
        )

        result_property_list: List[IProperty] = []
        # TODO: use dynamodb.batch_get_item ?
        for property_id in result_property_id_list:
            property_object = self.get_property_by_id(property_id)
            if property_object:
                result_property_list.append(property_object)

        return result_property_list, cast(Mapping[str, str] | None ,last_evaluated_key)





def run_save_test(table_name: str, region: str) -> None:
    # Load IProperty
    # Get the directory of the current script (data_reader.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Go up two levels to the project root, then into redfin_output
    python_project_folder = os.path.abspath(os.path.join(current_dir, ".."))
    redfin_output_path = os.path.join(python_project_folder, "crawler", "redfin_output", "redfin_properties_20250818_184641.jsonl")
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
            print(property)

            print("Start to save property to DynamoDB")
            dynamoDbService = DynamoDBPropertyService(table_name, region_name=region)
            new_property = IProperty(
                IProperty.generate_id(),
                metadata,
                history,
            )
            dynamoDbService._write_property(new_property)

            if count == 1:
                break

        # Check if entry exists
        print(f"Checking if the first property exists in DynamoDB")
        dynamoDbService.get_property_by_id(new_property.id)
        print(f"Finished processing. Total properties processed: {count}, errors logged to {error_log_file}")

def run_read_test(table_name: str, region: str, property_id: str) -> None:
    """
    Run a read test to retrieve a property by its ID from DynamoDB.

    Args:
        table_name (str): The name of the DynamoDB table.
        region (str): The AWS region where the DynamoDB table is located.
        property_id (str): The ID of the property to retrieve.
    """
    dynamoDbService = DynamoDBPropertyService(table_name, region_name=region)
    property_obj = dynamoDbService.get_property_by_id(property_id)
    if property_obj:
        print(f"Retrieved property: {property_obj}")
    else:
        print(f"Property with ID {property_id} not found")

if __name__ == "__main__":
    # Logging is already configured at module level
    # Set up logging when module is imported
    log_file_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    log_file_prefix = "dynamodb_service"
    logger_factory.configure_logger(
        log_file_path=log_file_dir,
        log_file_prefix=log_file_prefix,
    )
    logger = logger_factory.get_logger(__name__)

    # Dynamodb set up
    table_name = "properties"
    region = "us-west-2"

    # Query test
    dynamoDbService = DynamoDBPropertyService(table_name, region_name=region)
    query = PropertyQueryPattern(
        state = "WA",
        city_list= ["Seattle"],
        zip_code_list = [98109],
        status_list = [PropertyStatus.Active],
    )
    result = dynamoDbService._query_properties(
        PropertyStatus.Active,
        query,
        # limit=20,
    )

    logger.info(f"result count: {len(result[0])}")
    logger.info(f"last evaluate key: {result[1]}")
    properties = result[0]
    for property in properties:
        logger.info(property)

    # test_sk = "WA#Bellevue#98007#Condo"
    # parts = _parse_address_property_type_index(test_sk)
    # print(parts)




    # Write test
    # run_save_test(table_name, region)

    # Delete test
    # property_id = "25738d02-56df-4bd4-959e-144cd7eb5e12"
    # dynamoDbService = DynamoDBPropertyService(table_name, region_name=region)
    # dynamoDbService.delete_property_by_id(property_id)