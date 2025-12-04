
from pathlib import Path
from typing import Callable, List, Set
from datetime import datetime, timedelta
import requests
import time


import shared.logger_factory as logger_factory
from shared.iproperty import (
    IProperty,
    PropertyStatus,
    IPropertyHistory,
    PropertyHistoryEventType,
)
from data_service.dynamodb_property_service import (
    DynamoDBPropertyService,
)
from data_service.iproperty_service import (
    PropertyQueryPattern,
)
from crawler.redfin_spider.redfin_parser import (
    parse_property_page
)
from data_service.redfin_data_parser import (
    parse_property_history,
    parse_property_status,
    parse_raw_data_to_property,
)
from data_service.redfin_data_reader import (
    get_raw_data_entry,
)


def update_property(property: IProperty, dynamodb_service: DynamoDBPropertyService) -> None:

    # Set up logging
    logger = logger_factory.get_logger(__name__)
    logger.info(f"Property id in DB: {property.id}, address hash: {property.address.address_hash}")

    if not property.data_sources:
        raise ValueError(f"Empty data source for property: {property.id}, address: {property.address}")

    source_url = property.data_sources[0].source_url

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(source_url, headers=headers)
        item_dict = parse_property_page(
            url=source_url,
            html_content=response.text,
        )
        raw_data = get_raw_data_entry(item_dict)

        new_metadata, new_history = parse_raw_data_to_property(
            raw_data,
            property,
        )

        # # Parse history
        # new_history = parse_property_history(
        #     data = get_raw_data_entry(item_dict),
        #     address = property.address,
        #     last_updated = property.last_updated,
        # )
        # # Need to merge history first in case history is removed on closed property
        # property.update_history(new_history)

        # # Inactive property may have many data fields omitted, only update the status
        # status_raw_str = item_dict.get("status")
        # if not isinstance(status_raw_str, str):
        #     raise ValueError(f"Status: {status_raw_str} is not str")

        # new_status = parse_property_status(status_raw_str, property.history)
        # if new_status != property.status:
        #     logger.info(f"Found status update, new status: {new_status.value}, old status: {property.status.value}")
        #     property.metadata.update_status(new_status)

        #     # Reset property price if property is sold
        #     if new_status == PropertyStatus.Sold:
        #         property.metadata.update_price(None)




        logger.info(f"Property status: {property.status} for address hash: {property.address.address_hash}")

        dynamodb_service.create_or_update_property(
            new_metadata,
            new_history,
        )

    except Exception as e:
        logger.error(e)
        raise e

# TODO: remove the function after testing
def scan_and_update(
        query: PropertyQueryPattern,
        # callback: Callable[[IProperty], None],
        ) -> None:
    """
    Scan and update in one run. It doesn't handle failures
    """

    # Set up logging
    logger = logger_factory.get_logger(__name__)

    table_name = "properties"
    region = "us-west-2"
    dynamodb_service = DynamoDBPropertyService(table_name, region_name=region)

    # TODO: remove the check after last evaludate key fix
    if not query.status_list:
        raise ValueError("Status list cannot be empty")

    last_evaluated_key = None
    processed_count = 0
    while True:
        logger.info(f"scan_and_update, while loop begin, last_evaluated_key: {last_evaluated_key}")
        properties, last_evaluated_key = dynamodb_service.query_properties(
            query,
            exclusive_start_key=last_evaluated_key,
        )

        for index, property in enumerate(properties):
            update_property(property, dynamodb_service)
            processed_count += 1
            logger.info(f"Updated property count: {processed_count}")

            if (index + 1) % 200 == 0:
                delay_seconds = 60
                logger.info(f"Processed count: {index + 1}, sleep for {delay_seconds} seconds")
                time.sleep(delay_seconds)


        if not last_evaluated_key:
            break

    logger.info(f"Total processed count: {processed_count}")

# TODO: probably don't need query but just status? refactor to scan for multiptle status
def scan_and_update2(
        query: PropertyQueryPattern,
        property_id_file: str | None = None,
        last_evaluated_property_id: str | None = None,
        ) -> None:

    # Set up logging
    logger = logger_factory.get_logger(__name__)

    logger.info(f"Scan for query: {query}property_id_file: {property_id_file}, last_evaluated_property_id: {last_evaluated_property_id}")

    table_name = "properties"
    region = "us-west-2"
    dynamodb_service = DynamoDBPropertyService(table_name, region_name=region)

    # Read property ids from file if provided
    if property_id_file:
        with open(property_id_file, 'r', encoding='utf-8') as file:
            property_ids = [line.strip() for line in file if line.strip()]

        if last_evaluated_property_id:
            try:
                last_index = property_ids.index(last_evaluated_property_id)
                property_ids = property_ids[last_index + 1:]
            except ValueError:
                logger.warning(f"Last evaluated property ID {last_evaluated_property_id} not found in file. Starting from beginning.")
                return

        processed_count = 0
        for index, property_id in enumerate(property_ids):
            try:
                property = dynamodb_service.get_property_by_id(property_id)
                if property:
                    update_property(property, dynamodb_service)
                    processed_count += 1
                    logger.info(f"Updated property ID: {property_id}, total updated count: {processed_count}")
                else:
                    logger.warning(f"Property ID {property_id} not found in database.")
                    raise ValueError(f"Property ID {property_id} not found in database.")
            except Exception as e:
                logger.error(f"Error processing property ID {property_id}: {e}, last evaluated property ID: {property_ids[index - 1] if index > 0 else 'N/A'}")
                return
        logger.info(f"Total processed count: {processed_count}")
    else:
        # Get property id first
        property_id_file_prefix = "property_scan_ids"
        property_id_file_dir = Path(__file__).resolve().parent / "logs"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        property_id_file = str(property_id_file_dir / f"{property_id_file_prefix}_{timestamp}.txt")

        property_id_file_dir.mkdir(exist_ok=True)

        property_list: List[IProperty] = []
        with open(property_id_file, 'w', encoding='utf-8') as file:
            last_evaluated_key = None
            total_count = 0
            while True:
                logger.info(f"scan_and_update, while loop begin, last_evaluated_key: {last_evaluated_key}")
                properties, last_evaluated_key = dynamodb_service.query_properties(
                    query,
                    exclusive_start_key=last_evaluated_key,
                )
                property_list.extend(properties)

                for property in properties:
                    file.write(f"{property.id}\n")
                    total_count += 1

                if not last_evaluated_key:
                    break

        processed_count = 0
        for index, property in enumerate(property_list):
            try:
                update_property(property, dynamodb_service)
                processed_count += 1
                logger.info(f"Updated property count: {processed_count}")

                if (index + 1) % 200 == 0:
                    delay_seconds = 60
                    logger.info(f"Processed count: {index + 1}, sleep for {delay_seconds} seconds")
                    time.sleep(delay_seconds)
            except Exception as error:
                logger.error(f"Error processing property ID {property.id}: {error}, last evaluated property ID: {property_list[index - 1].id if index > 0 else 'N/A'}")
                return
        logger.info(f"Total processed count: {processed_count}")



def main() -> None:
    # Set up logging
    log_file_dir = str(Path(__file__).resolve().parent / "logs")
    logger_factory.configure_logger(
        log_file_path=log_file_dir,
        log_file_prefix="property_scan_tool",
        enable_file_logging=True,
    )
    logger = logger_factory.get_logger(__name__)

    query = PropertyQueryPattern(
        state="WA",
        status_list=[PropertyStatus.Active]
    )
    scan_and_update2(
        query,
        # property_id_file = "/Users/yunpenghou-macbookpro2023/workspace/houseTracker/python/tools/logs/property_scan_ids_20251202_205748.txt",
        # last_evaluated_property_id = "26ae0202-a10c-4c70-83d3-929336db01b2",
    )


if __name__ == "__main__":
    main()