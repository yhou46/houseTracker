"""
Property Store Tool
Store property data from files into DB
"""

from typing import List
import os
from datetime import datetime
import time
from pathlib import Path

import shared.logger_factory as logger_factory
from data_service.dynamodb_property_service import DynamoDBPropertyService
from data_service.redfin_data_reader import (
    IPropertyDataStream,
    PropertyDataStreamParsingError,
    RedfinFileDataReader,
)

def get_list_of_property_files(file_directory: str, start_file: str, end_file: str) -> List[str]:
    """
    Get a sorted list of files in file_directory between start_file and end_file (inclusive).
    If start_file or end_file is not found, return an empty list.
    The returned list includes both start_file and end_file. And the files doesn't have path included.
    """
    # Get all files in the directory
    all_files = [f for f in os.listdir(file_directory) if os.path.isfile(os.path.join(file_directory, f))]
    all_files.sort()

    try:
        start_idx = all_files.index(start_file)
        end_idx = all_files.index(end_file)
        if start_idx > end_idx:
            return []
    except ValueError:
        return []

    # Return files from start_idx to end_idx (inclusive)
    return all_files[start_idx : end_idx+1]

def store_property_from_file(
        property_file_name: str,
        data_reader_error_file: str,
        table_name: str,
        region: str,
        delay_seconds: int | None,
        delay_interval: int | None,
        max_update_count: int | None,
        ) -> None:
    """
    Store properties from a file into DynamoDB.

    Args:
        property_file_name (str): The path to the file containing property data.
        table_name (str): The name of the DynamoDB table.
        region (str): The AWS region where the DynamoDB table is located.
    """
    # Set up logging
    logger = logger_factory.get_logger(__name__)

    # Get the directory of the current script (data_reader.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Go up two levels to the project root, then into redfin_output
    python_project_folder = os.path.abspath(os.path.join(current_dir, ".."))
    property_data_file = os.path.join(python_project_folder, "crawler", "redfin_output", property_file_name)

    logger.info(f"Starting to read Redfin data from {property_data_file}. Error file: {data_reader_error_file}")

    with open(data_reader_error_file, 'w', encoding='utf-8') as error_file:
        def file_error_handler(error: PropertyDataStreamParsingError) -> None:
            error_msg = f"{datetime.now().isoformat()} - {str(error)}\n"
            error_file.write(error_msg)
            error_file.flush()

        reader: IPropertyDataStream = RedfinFileDataReader(property_data_file, file_error_handler)
        dynamoDbService = DynamoDBPropertyService(table_name, region_name=region)

        count = 0
        logger.info("Start to save property to DynamoDB")
        for metadata, history in reader:
            logger.info(f"Processing property with address: {metadata.address}, last updated: {metadata.last_updated}, count: {count}")

            # Update or create property
            dynamoDbService.create_or_update_property(metadata, history)

            count += 1
            if max_update_count and count >= max_update_count:
                logger.info(f"Reached max update count: {max_update_count}, stop processing further")
                break
            if delay_interval != None and delay_interval > 0 and count % delay_interval == 0:
                if delay_seconds != None and delay_seconds > 0:
                    logger.info(f"Processed count: {count}, sleep for {delay_seconds} seconds")
                    time.sleep(delay_seconds)
        logger.info(f"Finished processing. Total properties processed: {count}, reader errors logged to {data_reader_error_file}, service log file: {logger_factory.get_log_file_path()}")

def store_properties_to_db(
    property_file_dir: str,
    start_file: str,
    end_file: str,
    delay_seconds: int,
    delay_interval: int, # delay after processing this number of properties
) -> None:
    """
    Store properties from multiple files into DynamoDB.

    Args:
        property_file_dir (str): The directory containing property data files.
        start_file (str): The starting file name to process.
        end_file (str): The ending file name to process.
        delay_seconds (int): Number of seconds to delay after processing delay_interval files.
        delay_interval (int): Number of files to process before delaying.
    """
    # Set up logging
    logger = logger_factory.get_logger(__name__)

    table_name = "properties"
    region = "us-west-2"

    target_files = get_list_of_property_files(property_file_dir, start_file, end_file)
    logger.info(f"Found {len(target_files)} files to process from {property_file_dir}")
    # Add file path to each file
    targe_files_with_path = [os.path.join(property_file_dir, f) for f in target_files]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    python_project_folder = os.path.abspath(os.path.join(current_dir, ".."))
    error_log_file = os.path.join(python_project_folder, "tools", "logs", f"data_reader_errors_{timestamp}.log")

    for index, property_file in enumerate(targe_files_with_path):
        logger.info(f"Processing file {index + 1}/{len(targe_files_with_path)}: {property_file}")

        store_property_from_file(
            property_file,
            error_log_file,
            table_name,
            region,
            delay_seconds,
            delay_interval,
            max_update_count=None,
        )
        logger.info(f"Processed file {index + 1}/{len(targe_files_with_path)}: {property_file}, sleep for some time...")
        if index < len(targe_files_with_path) - 1:
            time.sleep(60)

def main() -> None:

    # Set up logging
    log_file_dir = str(Path(__file__).resolve().parent / "logs")
    logger_factory.configure_logger(
        log_file_path=log_file_dir,
        log_file_prefix="property_store_tool",
        enable_file_logging=True,
    )
    logger = logger_factory.get_logger(__name__)


    property_data_dir = str(Path(__file__).resolve().parent.parent / "crawler" / "redfin_output")

    # Edit files below
    start_file = "redfin_properties_20250917_175317.jsonl"
    end_file = "redfin_properties_20250917_175317.jsonl"

    files = get_list_of_property_files(property_data_dir, start_file, end_file)
    logger.info(f"Found {len(files)} files in {property_data_dir}")
    logger.info(f"Files: {files}")

    delay_seconds = 2
    delay_interval = 200
    store_properties_to_db(
        property_data_dir,
        start_file,
        end_file,
        delay_seconds,
        delay_interval,
    )

if __name__ == "__main__":
    main()