import boto3
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from decimal import Decimal
import re
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError

from data_service.dynamodb_property_service import (
    DynamoDBServiceForProperty,
    DynamoDbPropertyTableAttributeName,
    DynamoDbPropertyTableEntityType,
    get_sk_from_entity,
    get_history_event_id_from_sk
)

# TODO: remove this file after testing is done

class TimezoneFixer:
    """
    Fixes timezone issues in DynamoDB history records by converting Pacific time to UTC.
    """

    def __init__(self, table_name: str, region_name: str = "us-west-2"):
        """
        Initialize the timezone fixer.

        Args:
            table_name: DynamoDB table name
            region_name: AWS region name
        """
        self.table_name = table_name
        self.region_name = region_name
        self.dynamodb_client = boto3.client('dynamodb', region_name=region_name)
        self.dynamodb_resource = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb_resource.Table(table_name)

        # Set up logging
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Set up logging for the timezone fix operation."""
        # Create logs directory if it doesn't exist
        current_dir = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(current_dir, "timezone_fix_logs")
        os.makedirs(logs_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Fix log
        fix_log_file = os.path.join(logs_dir, f"timezone_fix_{timestamp}.log")
        self.fix_logger = logging.getLogger(f"timezone_fix_{timestamp}")
        self.fix_logger.setLevel(logging.INFO)
        fix_handler = logging.FileHandler(fix_log_file)
        fix_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.fix_logger.addHandler(fix_handler)

        # Error log
        error_log_file = os.path.join(logs_dir, f"timezone_fix_errors_{timestamp}.log")
        self.error_logger = logging.getLogger(f"timezone_fix_errors_{timestamp}")
        self.error_logger.setLevel(logging.ERROR)
        error_handler = logging.FileHandler(error_log_file)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.error_logger.addHandler(error_handler)

        self.fix_logger.info(f"Timezone fix operation started. Table: {self.table_name}, Region: {self.region_name}")
        self.fix_logger.info(f"Fix log: {fix_log_file}")
        self.fix_logger.info(f"Error log: {error_log_file}")

    def _needs_timezone_fix(self, sk: str, time_str: str) -> bool:
        """
        Check if an SK needs timezone fixing.

        Args:
            sk: Sort key string

        Returns:
            bool: True if the SK needs timezone fixing
        """
        # Check if it's a history SK
        if not sk.startswith(f"{DynamoDbPropertyTableEntityType.PropertyHistory.value}#"):
            return False

        # Check if it ends with a datetime without timezone
        # Pattern: HISTORY#event_id#YYYY-MM-DDTHH:MM:SS (no timezone suffix)
        parts = sk.split("#")
        if len(parts) < 3:
            return False

        datetime_part = parts[2]

        # Check if it matches the pattern without timezone
        # Should match: YYYY-MM-DDTHH:MM:SS (no + or - timezone)
        datetime_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        return bool(re.match(datetime_pattern, datetime_part)) and bool(re.match(datetime_pattern, time_str))

    def _extract_datetime_from_sk(self, sk: str) -> Optional[datetime]:
        """
        Extract datetime from SK string.

        Args:
            sk: Sort key string

        Returns:
            datetime: Extracted datetime or None if parsing fails
        """
        try:
            parts = sk.split("#")
            if len(parts) < 3:
                return None

            datetime_str = parts[2]
            return datetime.fromisoformat(datetime_str)
        except ValueError as e:
            self.error_logger.error(f"Failed to parse datetime from SK {sk}: {e}")
            return None

    def _convert_pacific_to_utc(self, pacific_datetime: datetime) -> datetime:
        """
        Convert Pacific time to UTC.

        Args:
            pacific_datetime: Datetime in Pacific time

        Returns:
            datetime: Datetime in UTC
        """
        # Create timezone-aware datetime in Pacific time
        pacific_tz = ZoneInfo("America/Los_Angeles")
        pacific_aware = pacific_datetime.replace(tzinfo=pacific_tz)

        # Convert to UTC
        utc_datetime = pacific_aware.astimezone(timezone.utc)

        return utc_datetime

    def _create_new_sk(self, old_sk: str, new_datetime: datetime) -> str:
        """
        Create new SK with UTC datetime.

        Args:
            old_sk: Original sort key
            new_datetime: New UTC datetime

        Returns:
            str: New sort key with UTC datetime
        """
        parts = old_sk.split("#")
        if len(parts) < 3:
            raise ValueError(f"Invalid SK format: {old_sk}")

        event_id = parts[1]
        new_sk = f"{DynamoDbPropertyTableEntityType.PropertyHistory.value}#{event_id}#{new_datetime.isoformat()}"
        return new_sk

    def scan_property_items(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Scan DynamoDB table for property items.

        Args:
            batch_size: Number of items to process in each batch

        Returns:
            List of property items
        """
        items = []
        last_evaluated_key = None

        self.fix_logger.info(f"Starting scan for property items...")

        while True:
            try:
                if last_evaluated_key:
                    response = self.table.scan(
                        FilterExpression=boto3.dynamodb.conditions.Attr('SK').begins_with(
                            f"{DynamoDbPropertyTableEntityType.Property.value}#"
                        ),
                        Limit=batch_size,
                        ExclusiveStartKey=last_evaluated_key
                    )
                else:
                    response = self.table.scan(
                        FilterExpression=boto3.dynamodb.conditions.Attr('SK').begins_with(
                            f"{DynamoDbPropertyTableEntityType.Property.value}#"
                        ),
                        Limit=batch_size
                    )
                batch_items = response.get('Items', [])
                items.extend(batch_items)

                self.fix_logger.info(f"Scanned batch of {len(batch_items)} items, total so far: {len(items)}")

                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break

            except ClientError as e:
                self.error_logger.error(f"Error scanning table: {e}")
        return items

    def analyze_property_items(self, items: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        # find duplicate property metadata
        property_map: Dict[str, List[Any]] = dict()
        for item in items:
            property_id = item.get(DynamoDbPropertyTableAttributeName.Id.value)
            if not property_id:
                raise ValueError(f"Item missing Id attribute: {item}")
            if property_map.get(property_id) != None:
                property_map[property_id].append(item)
            else:
                property_map[property_id] = [item]
        self.fix_logger.info(f"total unique property metadata count: {len(property_map)}")
        duplicates = {k: v for k, v in property_map.items() if len(v) > 1}
        self.fix_logger.info(f"duplicate property metadata count: {len(duplicates)}, metadata: {duplicates}")
        return duplicates

    def analyze_property_items_with_time(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # find property item which SK has update time
        property_map: Dict[str, List[Any]] = dict()
        for item in items:
            property_id = item.get(DynamoDbPropertyTableAttributeName.Id.value)
            if not property_id:
                raise ValueError(f"Item missing Id attribute: {item}")
            if property_map.get(property_id) != None:
                property_map[property_id].append(item)
            else:
                property_map[property_id] = [item]
        self.fix_logger.info(f"total unique property metadata count: {len(property_map)}")
        duplicates = {k: v for k, v in property_map.items() if len(v) > 1}
        self.fix_logger.info(f"duplicate property metadata count: {len(duplicates)}, metadata: {duplicates}")

        if len(duplicates) > 0:
            raise ValueError(f"Found duplicate property metadata, cannot proceed with time fix. Duplicates: {duplicates}")

        property_with_time = []
        for item in items:
            sk = item.get(DynamoDbPropertyTableAttributeName.SK.value)
            if not sk:
                raise ValueError(f"Item missing SK attribute: {item}")
            # Check if SK has update time
            parts = sk.split("#")
            if len(parts) > 2:
                property_with_time.append(item)
        return property_with_time

    def update_property_items_without_update_time(self, items: List[Dict[str, Any]], max_count: int | None = None) -> None:
        # Update property items to remove update time from SK
        processed_count = 0
        for item in items:
            pk = item.get(DynamoDbPropertyTableAttributeName.PK.value)
            sk = item.get(DynamoDbPropertyTableAttributeName.SK.value)
            if not isinstance(sk, str):
                self.error_logger.error(f"Item SK is not a string, cannot process: {item}")
                break
            elements = sk.split("#")
            if len(elements) != 3:
                raise ValueError(f"Invalid SK format: {sk}")
            new_sk = f"{elements[0]}#{elements[1]}"
            item[DynamoDbPropertyTableAttributeName.SK.value] = new_sk
            try:
                self.table.put_item(Item=item)
                self.table.delete_item(Key = {
                        'PK': pk,
                        'SK': sk,
                    })
                self.fix_logger.info(f"Updated item PK: {pk}, old SK: {sk}, new SK: {new_sk}")
            except Exception as e:
                self.error_logger.error(f"Failed to update item PK: {pk}, SK: {sk}: {e}")
            processed_count += 1
            if max_count and processed_count >= max_count:
                self.fix_logger.info(f"Reached max_count of {max_count}, stopping further processing.")
                break

    def remove_duplicate_property_metadata(self, items: Dict[str, List[Any]], max_count: int | None = None) -> None:
        # Scan and check if duplicates are valid
        processed_count = 0
        for property_id, item_list in items.items():
            self.fix_logger.info(f"Processing property_id: {property_id} with {len(item_list)} duplicates")
            # Sort by LastUpdated time, keep the latest one
            try:
                item_list.sort(key=lambda x: datetime.fromisoformat(x.get(DynamoDbPropertyTableAttributeName.LastUpdated.value)), reverse=True)

                # Check if all items have the same PK and address hash
                pk_set = {item.get(DynamoDbPropertyTableAttributeName.PK.value) for item in item_list}
                address_hash_set = {item.get(DynamoDbPropertyTableAttributeName.AddressHash.value) for item in item_list}
                if len(pk_set) > 1 or len(address_hash_set) > 1:
                    self.error_logger.error(f"Items for property_id {property_id} have inconsistent PKs or AddressHashes: PKs={pk_set}, AddressHashes={address_hash_set}")
                    raise ValueError(f"Inconsistent PKs or AddressHashes for property_id {property_id}")

            except Exception as e:
                self.error_logger.error(f"Failed to sort items by LastUpdated for property_id {property_id}: {e}")
                continue
            # Keep the first one, delete the rest
            items_to_delete = item_list[1:]
            for item in items_to_delete:
                pk = item.get(DynamoDbPropertyTableAttributeName.PK.value)
                sk = item.get(DynamoDbPropertyTableAttributeName.SK.value)
                if not pk or not sk:
                    self.error_logger.error(f"Item missing PK or SK, cannot delete: {item}")
                    continue
                try:
                    self.table.delete_item(
                        Key={
                            DynamoDbPropertyTableAttributeName.PK.value: pk,
                            DynamoDbPropertyTableAttributeName.SK.value: sk
                        }
                    )
                    self.fix_logger.info(f"Deleted duplicate item with PK: {pk}, SK: {sk}")
                except Exception as e:
                    self.error_logger.error(f"Failed to delete item with PK: {pk}, SK: {sk}: {e}")
            processed_count += 1
            if max_count and processed_count >= max_count:
                self.fix_logger.info(f"Reached max_count of {max_count}, stopping further processing.")
                break


    def scan_history_items(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Scan DynamoDB table for history items that need timezone fixing.

        Args:
            batch_size: Number of items to process in each batch

        Returns:
            List of items that need timezone fixing
        """
        items_to_fix = []
        items_not_need_fix = []
        last_evaluated_key = None

        self.fix_logger.info(f"Starting scan for history items that need timezone fixing...")

        while True:
            try:
                if last_evaluated_key:
                    response = self.table.scan(
                        FilterExpression=boto3.dynamodb.conditions.Attr('SK').begins_with(
                            f"{DynamoDbPropertyTableEntityType.PropertyHistory.value}#"
                        ),
                        Limit=batch_size,
                        ExclusiveStartKey=last_evaluated_key
                    )
                else:
                    response = self.table.scan(
                        FilterExpression=boto3.dynamodb.conditions.Attr('SK').begins_with(
                            f"{DynamoDbPropertyTableEntityType.PropertyHistory.value}#"
                        ),
                        Limit=batch_size
                    )
                items = response.get('Items', [])


                # Filter items that need timezone fixing
                for item in items:
                    sk = str(item.get(DynamoDbPropertyTableAttributeName.SK.value, ''))
                    time_str = str(item.get(DynamoDbPropertyTableAttributeName.HistoryEventDatetime.value, ''))
                    need_fix = self._needs_timezone_fix(sk, time_str)
                    if need_fix:
                        items_to_fix.append(item)
                    else:
                        items_not_need_fix.append(item)
                        self.fix_logger.info(f"items that doesn't need fix: {item}")

                self.fix_logger.info(f"Scanned batch of {len(items)} items, found {len(items_to_fix)} items needing fix")

                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break

            except ClientError as e:
                self.error_logger.error(f"Error scanning table: {e}")
                raise

        self.fix_logger.info(f"Scan complete. Total items needing timezone fix: {len(items_to_fix)}")
        return items_to_fix

    def analyze_items(self, items: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], str, datetime, datetime]]:
        """
        Analyze items and prepare fix information.

        Args:
            items: List of items to analyze

        Returns:
            List of tuples: (item, old_sk, old_datetime, new_utc_datetime)
        """
        fix_plan = []

        for item in items:
            pk = str(item.get(DynamoDbPropertyTableAttributeName.PK.value, ''))
            sk = str(item.get(DynamoDbPropertyTableAttributeName.SK.value, ''))

            # Extract old datetime
            old_datetime = self._extract_datetime_from_sk(sk)
            old_history_time = datetime.fromisoformat(str(item.get(DynamoDbPropertyTableAttributeName.HistoryEventDatetime.value, '')))
            if old_datetime != old_history_time:
                self.error_logger.error(f"old_sk_time: {old_datetime} is not equal to history time: {old_history_time}for pk: {pk}")
            if old_datetime is None:
                self.error_logger.error(f"Could not extract datetime from SK: {sk}")
                continue

            # Convert to UTC
            new_utc_datetime = self._convert_pacific_to_utc(old_datetime)

            fix_plan.append((item, sk, old_datetime, new_utc_datetime))

        return fix_plan

    def dry_run(self, batch_size: int = 100) -> None:
        """
        Perform a dry run to show what changes would be made.

        Args:
            batch_size: Number of items to process in each batch
        """
        self.fix_logger.info("=== STARTING DRY RUN ===")

        try:
            # Scan for items that need fixing
            items_to_fix = self.scan_history_items(batch_size)

            if not items_to_fix:
                self.fix_logger.info("No items found that need timezone fixing.")
                return

            # Analyze items
            fix_plan = self.analyze_items(items_to_fix)

            # Display summary
            self.fix_logger.info(f"\n=== DRY RUN SUMMARY ===")
            self.fix_logger.info(f"Total items to fix: {len(fix_plan)}")

            # Show sample changes
            self.fix_logger.info(f"\n=== SAMPLE CHANGES ===")
            for i, (item, old_sk, old_datetime, new_utc_datetime) in enumerate(fix_plan[:5]):
                pk = item.get(DynamoDbPropertyTableAttributeName.PK.value, '')
                new_sk = self._create_new_sk(old_sk, new_utc_datetime)

                self.fix_logger.info(f"Item {i+1}:")
                self.fix_logger.info(f"  PK: {pk}")
                self.fix_logger.info(f"  Old SK: {old_sk}")
                self.fix_logger.info(f"  New SK: {new_sk}")
                self.fix_logger.info(f"  Old datetime (Pacific): {old_datetime}")
                self.fix_logger.info(f"  New datetime (UTC): {new_utc_datetime}")
                # self.fix_logger.info(f"  Time difference: {new_utc_datetime - old_datetime}")
                self.fix_logger.info("")

            if len(fix_plan) > 5:
                self.fix_logger.info(f"... and {len(fix_plan) - 5} more items")

            # Show timezone conversion examples
            self.fix_logger.info(f"\n=== TIMEZONE CONVERSION EXAMPLES ===")
            pacific_tz = ZoneInfo("America/Los_Angeles")
            sample_times = [
                datetime(2025, 1, 15, 12, 0, 0),  # Winter (PST)
                datetime(2025, 7, 15, 12, 0, 0),  # Summer (PDT)
                datetime(2025, 3, 9, 2, 30, 0),   # DST transition
                datetime(2025, 11, 2, 2, 30, 0),  # DST transition
            ]

            for sample_time in sample_times:
                utc_time = self._convert_pacific_to_utc(sample_time)
                # self.fix_logger.info(f"Pacific: {sample_time} -> UTC: {utc_time} (offset: {utc_time - sample_time})")

            self.fix_logger.info(f"\n=== DRY RUN COMPLETE ===")
            self.fix_logger.info(f"To apply these changes, run with dry_run=False")

        except Exception as e:
            self.error_logger.error(f"Error during dry run: {e}")
            raise

    def update_history_item(self, item: Dict[str, Any], old_sk: str, new_utc_datetime: datetime) -> bool:
        """
        Update a history item with new UTC datetime in SK.

        Args:
            item: The DynamoDB item to update
            old_sk: Original sort key
            new_utc_datetime: New UTC datetime

        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            pk = item.get(DynamoDbPropertyTableAttributeName.PK.value, '')
            new_sk = self._create_new_sk(old_sk, new_utc_datetime)

            # Create new item with updated SK
            updated_item = item.copy()
            updated_item[DynamoDbPropertyTableAttributeName.SK.value] = new_sk
            updated_item[DynamoDbPropertyTableAttributeName.HistoryEventDatetime.value] = new_utc_datetime.isoformat()

            # Update the HistoryEventDatetime attribute as well
            if DynamoDbPropertyTableAttributeName.HistoryEventDatetime.value in updated_item:
                updated_item[DynamoDbPropertyTableAttributeName.HistoryEventDatetime.value] = new_utc_datetime.isoformat()

            # Delete old item and insert new item
            # TODO: bug: need to batch process them
            self.table.delete_item(
                Key={
                    DynamoDbPropertyTableAttributeName.PK.value: pk,
                    DynamoDbPropertyTableAttributeName.SK.value: old_sk
                }
            )

            self.table.put_item(Item=updated_item)

            self.fix_logger.info(f"Successfully updated item: PK={pk}, Old SK={old_sk}, New SK={new_sk}")
            return True

        except Exception as e:
            self.error_logger.error(f"Failed to update item with PK={pk}, SK={old_sk}: {e}")
            return False

    def run_fix(self, batch_size: int = 100, max_items: int| None = 10 ) -> None:
        """
        Run the actual timezone fix operation.

        Args:
            max_items: Maximum number of items to fix (for testing)
            batch_size: Number of items to process in each batch
        """
        self.fix_logger.info("=== STARTING ACTUAL FIX OPERATION ===")
        self.fix_logger.info(f"Max items to fix: {max_items}")

        try:
            # Scan for items that need fixing
            items_to_fix = self.scan_history_items(batch_size)

            if not items_to_fix:
                self.fix_logger.info("No items found that need timezone fixing.")
                return

            # Limit to max_items for testing
            if max_items != None:
                items_to_fix = items_to_fix[:max_items]
                self.fix_logger.info(f"Limited to {len(items_to_fix)} items for testing")

            # Analyze items
            fix_plan = self.analyze_items(items_to_fix)

            if not fix_plan:
                self.fix_logger.info("No items to fix after analysis.")
                return

            # Display summary
            self.fix_logger.info(f"\n=== FIX OPERATION SUMMARY ===")
            self.fix_logger.info(f"Total items to fix: {len(fix_plan)}")

            # Perform updates
            success_count = 0
            failure_count = 0

            self.fix_logger.info(f"\n=== STARTING UPDATES ===")
            for i, (item, old_sk, old_datetime, new_utc_datetime) in enumerate(fix_plan):
                self.fix_logger.info(f"Processing item {i+1}/{len(fix_plan)}")

                if self.update_history_item(item, old_sk, new_utc_datetime):
                    success_count += 1
                else:
                    failure_count += 1

                # Add a small delay to avoid overwhelming DynamoDB
                import time
                time.sleep(0.1)

            self.fix_logger.info(f"\n=== FIX OPERATION COMPLETE ===")
            self.fix_logger.info(f"Successfully updated: {success_count} items")
            self.fix_logger.info(f"Failed to update: {failure_count} items")
            self.fix_logger.info(f"Total processed: {len(fix_plan)} items")

        except Exception as e:
            self.error_logger.error(f"Error during fix operation: {e}")
            raise


def run_timezone_fix_dry_run(table_name: str, region_name: str = "us-west-2") -> None:
    """
    Run a dry run of the timezone fix operation.

    Args:
        table_name: DynamoDB table name
        region_name: AWS region name
    """
    fixer = TimezoneFixer(table_name, region_name)
    fixer.dry_run()


def run_timezone_fix_actual(table_name: str, region_name: str = "us-west-2", max_items: int | None = 10) -> None:
    """
    Run the actual timezone fix operation.

    Args:
        table_name: DynamoDB table name
        region_name: AWS region name
        max_items: Maximum number of items to fix (for testing)
    """
    fixer = TimezoneFixer(table_name, region_name)
    fixer.run_fix(max_items=max_items)

def remove_property_duplicates(table_name: str, region_name: str = "us-west-2") -> None:
    """
    Dry run to identify duplicate property metadata entries.

    Args:
        table_name: DynamoDB table name
        region_name: AWS region name
    """
    fixer = TimezoneFixer(table_name, region_name)
    items = fixer.scan_property_items()
    duplicates = fixer.analyze_property_items(items)
    if not duplicates:
        fixer.fix_logger.info("No duplicate property metadata entries found.")
    else:
        fixer.fix_logger.info(f"Found {len(duplicates)} duplicate property metadata entries.")

    # fixer.remove_duplicate_property_metadata(duplicates)

def update_property_with_update_time(table_name: str, region_name: str = "us-west-2", max_count: int | None = 10) -> None:
    """
    Update property metadata entries to include update time in SK.

    Args:
        table_name: DynamoDB table name
        region_name: AWS region name
        max_count: Maximum number of properties to update (for testing)
    """
    fixer = TimezoneFixer(table_name, region_name)
    items = fixer.scan_property_items()
    property_with_time = fixer.analyze_property_items_with_time(items)
    if not property_with_time:
        fixer.fix_logger.info("No property metadata entries with update time found.")
    else:
        fixer.fix_logger.info(f"Found {len(property_with_time)} property metadata entries with update time.")

    # fixer.update_property_items_without_update_time(property_with_time, max_count=max_count)

if __name__ == "__main__":
    # Set up basic logging for console output
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Configuration
    table_name = "properties"
    region_name = "us-west-2"

    # Run actual fix on 10 records for testing
    # run_timezone_fix_dry_run(table_name, region_name)
    update_property_with_update_time(table_name, region_name, max_count=None)
