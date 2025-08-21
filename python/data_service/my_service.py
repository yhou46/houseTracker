from typing import List, Optional, Literal, Dict, Any
from datetime import datetime, timezone
import json
import logging
from enum import Enum
import os
from decimal import Decimal

import boto3
from mypy_boto3_dynamodb.type_defs import AttributeDefinitionTypeDef, KeySchemaElementTypeDef, GlobalSecondaryIndexTypeDef, GlobalSecondaryIndexOutputTypeDef
from mypy_boto3_dynamodb.client import DynamoDBClient
from botocore.exceptions import ClientError


from shared.iproperty import IProperty, PropertyType, IPropertyHistory, IPropertyHistoryEvent, IPropertyAddress

from data_service.redfin_data_reader import RedfinFileDataReader, PropertyDataStreamParsingError, IPropertyDataStream

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


def get_pk_from_entity(entity_id: str, entity_type: DynamoDbPropertyTableEntityType) -> str:
    return f"{entity_type.value}#{entity_id}"

def get_sk_from_entity(entity_id: str, entity_type: DynamoDbPropertyTableEntityType, time_in_utc: datetime) -> str:
    return f"{entity_type.value}#{entity_id}#{time_in_utc.isoformat()}"

def get_address_property_type_index(state: str, zip_code: str, city: str, property_type: PropertyType) -> str:
    return f"{state}#{city}#{zip_code}#{property_type.value}"

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

def convert_property_history_to_dynamodb_item(history: IPropertyHistory) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    # Set up partition key and sort key
    for event in history.history:
        item = convert_property_history_event_to_dynamodb_item(history.property_id, event)
        items.append(item)
    return items

"""
Convert IProperty object to a DynamoDB item format.
NOTE: DynamoDB does not support float but only Decimal.
"""
def convert_property_to_dynamodb_items(property: IProperty) -> List[Dict[str, Any]]:
    property_item: Dict[str, Any] = dict()

    # Set up partition key and sort key


    property_item[DynamoDbPropertyTableAttributeName.PK.value] = get_pk_from_entity(property.id, DynamoDbPropertyTableEntityType.Property)
    property_item[DynamoDbPropertyTableAttributeName.SK.value] = get_sk_from_entity(property.id, DynamoDbPropertyTableEntityType.Property, property.last_updated)

    # Set up global secondary indexes
    # Check table creation for attribute details
    property_item[DynamoDbPropertyTableAttributeName.AddressPropertyTypeIndex.value] = get_address_property_type_index(property.address.state, property.address.zip_code, property.address.city, property.property_type)
    property_item[DynamoDbPropertyTableAttributeName.AddressHash.value] = property.address.get_address_hash()
    property_item[DynamoDbPropertyTableAttributeName.Status.value] = property.status.value

    # Other property entities
    property_item[DynamoDbPropertyTableAttributeName.Id.value] = property.id
    property_item[DynamoDbPropertyTableAttributeName.Address.value] = {
        DynamoDbPropertyTableAttributeName.Address_StreetName.value: property.address.street_name,
        DynamoDbPropertyTableAttributeName.Address_Unit.value: property.address.unit,
        DynamoDbPropertyTableAttributeName.Address_City.value: property.address.city,
        DynamoDbPropertyTableAttributeName.Address_State.value: property.address.state,
        DynamoDbPropertyTableAttributeName.Address_ZipCode.value: property.address.zip_code
    }
    property_item[DynamoDbPropertyTableAttributeName.Area.value] = {
        DynamoDbPropertyTableAttributeName.Area_Value.value: Decimal(property.area.value),
        DynamoDbPropertyTableAttributeName.Area_Unit.value: property.area.unit.value
    } if property.area else None
    property_item[DynamoDbPropertyTableAttributeName.PropertyType.value] = property.property_type.value
    property_item[DynamoDbPropertyTableAttributeName.LotArea.value] = {
        DynamoDbPropertyTableAttributeName.LotArea_Value.value: Decimal(property.lot_area.value),
        DynamoDbPropertyTableAttributeName.LotArea_Unit.value: property.lot_area.unit.value
    } if property.lot_area else None
    property_item[DynamoDbPropertyTableAttributeName.NumberOfBedrooms.value] = Decimal(property.number_of_bedrooms) if property.number_of_bedrooms is not None else None
    property_item[DynamoDbPropertyTableAttributeName.NumberOfBathrooms.value] = Decimal(property.number_of_bathrooms) if property.number_of_bathrooms is not None else None
    property_item[DynamoDbPropertyTableAttributeName.YearBuilt.value] = property.year_built
    property_item[DynamoDbPropertyTableAttributeName.Price.value] = Decimal(property.price) if property.price is not None else None
    property_item[DynamoDbPropertyTableAttributeName.LastUpdated.value] = property.last_updated.isoformat()
    property_item[DynamoDbPropertyTableAttributeName.DataSources.value] = [
        {
            DynamoDbPropertyTableAttributeName.DataSource_SourceId.value: ds.source_id,
            DynamoDbPropertyTableAttributeName.DataSource_SourceUrl.value: ds.source_url,
            DynamoDbPropertyTableAttributeName.DataSource_SourceName.value: ds.source_name
        } for ds in property.data_sources
    ]

    # Convert history
    history_items = convert_property_history_to_dynamodb_item(property.history)
    
    # Combine property item with history items
    property_items = [property_item] + history_items

    return property_items

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
        ):
    dynamodb_resource = boto3.resource('dynamodb', region_name=region_name)
    dynamodb_client = boto3.client('dynamodb', region_name=region_name)

    # Check if table already exists
    if check_dynamodb_table_exists(table_name, dynamodb_client):
        print(f"Table {table_name} already exists.")
        return

    # Define key schema
    key_schema: List[KeySchemaElementTypeDef] = [
        {"AttributeName": "PK", "KeyType": "HASH"},
        {"AttributeName": "SK", "KeyType": "RANGE"},
    ]

    # TODO: need to use a class to define the table schema, indexes, and attributes
    # Define Global Secondary Indexes
    global_secondary_indexes: List[GlobalSecondaryIndexTypeDef | GlobalSecondaryIndexOutputTypeDef] = [
        {
            "IndexName": "StatusAddressPropertyTypeIndex",
            "KeySchema": [
                {"AttributeName": "Status", "KeyType": "HASH"},
                {"AttributeName": "AddressPropertyTypeIndex", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        },
        {
            "IndexName": "AddressHashIndex",
            "KeySchema": [
                {"AttributeName": "AddressHash", "KeyType": "HASH"},
            ],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        }
    ]

    attribute_definitions: List[AttributeDefinitionTypeDef] = [
        {"AttributeName": "PK", "AttributeType": "S"}, # EntityType#EntityId
        {"AttributeName": "SK", "AttributeType": "S"}, # EntityType#EntityId#TimeInUtc
        {"AttributeName": "Status", "AttributeType": "S"},
        {"AttributeName": "AddressPropertyTypeIndex", "AttributeType": "S"}, # State#City#Zip#PropertyType
        {"AttributeName": "AddressHash", "AttributeType": "S"},
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

class DynamoDBServiceForProperty:
    def __init__(self, table_name: str, region_name: str = "us-west-2"):
        """
        Initialize DynamoDB service
        
        Args:
            table_name: DynamoDB table name (defaults to schema default)
            region_name: AWS region name
        """
        self.table_name = table_name
        self.dynamodb_client = boto3.client('dynamodb', region_name=region_name)
        if not check_dynamodb_table_exists(table_name, self.dynamodb_client):
            raise ValueError(f"DynamoDB table: {table_name} does not exist in region {region_name}.")
        print(f"After check DynamoDB table {table_name} exists in region {region_name}.")
        self.dynamodb_resource = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb_resource.Table(self.table_name)
        self.logger = logging.getLogger(__name__)

    def get_property_by_id(self, property_id: str) -> List[Dict[str, Any]]:
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

            # TODO: need to convert items to IProperty object
            for item in items:
                print(item)
            return items if items else []
        except ClientError as error:
            self.logger.error(f"Error retrieving property with ID {property_id}: {error.response['Error']['Message']}")
            raise error
    
    def _query_items_by_property_id(self, property_id: str) -> List[Dict[str, Any]]:
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

            for item in items:
                print(item)
            return items if items else []
        except ClientError as error:
            self.logger.error(f"Error retrieving property with ID {property_id}: {error.response['Error']['Message']}")
            raise error
    
    def get_property_by_address(self, address: IPropertyAddress):
        response = self.table.query(
            IndexName="AddressHashIndex",
            KeyConditionExpression=boto3.dynamodb.conditions.Key("AddressHash").eq(address.get_address_hash()),
        )
        # TODO: need to query the full property use the id
        items = response['Items']
        for item in items:
            print(item)

    # TODO: need to check if the property already exists and merge the history if possible; do NOT overwrite the existing property
    def save_property(self, property: IProperty):
        items = convert_property_to_dynamodb_items(property)
        print(f"Number of items to save: {len(items)}")
        try:
            with self.table.batch_writer() as writer:
                for item in items:
                    writer.put_item(Item=item)
        except ClientError as err:
            self.logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise err

    def delete_property_by_id(self, property_id: str):
        """
        Delete a property by its ID from the DynamoDB table.

        Args:
            property_id (str): The ID of the property to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        try:
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

def run_save_test(table_name: str, region: str):
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

        for property in reader:
            count += 1
            print(property)

            print("Start to save property to DynamoDB")
            dynamoDbService = DynamoDBServiceForProperty(table_name, region_name=region)
            dynamoDbService.save_property(property)
            
            if count == 1:
                break

        # Check if entry exists
        print(f"Checking if the first property exists in DynamoDB")
        dynamoDbService.get_property_by_id(property.id)
        print(f"Finished processing. Total properties processed: {count}, errors logged to {error_log_file}")

def run_read_test(table_name: str, region: str, property_id: str):
    """
    Run a read test to retrieve a property by its ID from DynamoDB.
    
    Args:
        table_name (str): The name of the DynamoDB table.
        region (str): The AWS region where the DynamoDB table is located.
        property_id (str): The ID of the property to retrieve.
    """
    dynamoDbService = DynamoDBServiceForProperty(table_name, region_name=region)
    dynamoDbService.get_property_by_id(property_id)

if __name__ == "__main__":
    # Dynamodb set up
    table_name = "properties"
    region = "us-west-2"

    # Write test
    run_save_test(table_name, region)

    # Read test
    # property_id = "f167cf57-db57-406b-9fa8-9ca566741b20"
    # address_str = "655 Crockett St Unit B304, Seattle, WA 98109"
    # address_obj = IPropertyAddress(address_str)
    # dynamoDbService = DynamoDBServiceForProperty(table_name, region_name=region)
    # dynamoDbService.get_property_by_address(address_obj)

    # Delete test
    # property_id = "f167cf57-db57-406b-9fa8-9ca566741b20"
    # dynamoDbService = DynamoDBServiceForProperty(table_name, region_name=region)
    # dynamoDbService.delete_property_by_id(property_id)