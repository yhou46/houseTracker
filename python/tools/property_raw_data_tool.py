

import os
import json
from pathlib import Path

import shared.logger_factory as logger_factory
from data_service.aws_s3_util import (
    upload_json_objects
)
from tools.property_store_tool import get_list_of_files

def store_raw_data_to_storage(
    s3_bucket_name: str,
    property_file_dir: str,
    start_file: str,
    end_file: str,
) -> None:
    """
    Store raw property data files to S3 storage.

    Args:
        property_file_dir (str): Directory containing property data files.
        start_file (str): Starting file name.
        end_file (str): Ending file name.
    """
    logger = logger_factory.get_logger(__name__)

    files = get_list_of_files(property_file_dir, start_file, end_file)
    logger.info(f"Found {len(files)} files in {property_file_dir} to upload to S3.")

    for file_name in files:
        file_path = os.path.join(property_file_dir, file_name)
        logger.info(f"Uploading file {file_path} to S3.")

        # Read JSON objects from the file
        json_objects = []
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                json_object = json.loads(line)
                json_objects.append(json_object)

        upload_json_objects(
            json_objects,
            s3_bucket_name,
        )

def main() -> None:

    # Set up logging
    log_file_dir = str(Path(__file__).resolve().parent / "logs")
    logger_factory.configure_logger(
        log_file_path=log_file_dir,
        log_file_prefix="property_raw_data_tool",
        enable_file_logging=True,
    )
    logger = logger_factory.get_logger(__name__)


    property_data_dir = str(Path(__file__).resolve().parent.parent / "crawler" / "redfin_spider" / "redfin_spider_monolith_output")

    # Edit files below
    start_file = "redfin_properties_20251207_152104.jsonl"
    end_file = "redfin_properties_20251211_192526.jsonl"

    files = get_list_of_files(property_data_dir, start_file, end_file)
    logger.info(f"Found {len(files)} files in {property_data_dir}")
    logger.info(f"Files: {files}")

    s3_bucket_name = "myhousetracker-99ce79"
    store_raw_data_to_storage(
        s3_bucket_name,
        property_data_dir,
        start_file,
        end_file,
    )

if __name__ == "__main__":
    main()