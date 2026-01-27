

import os
import json
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
)

import shared.logger_factory as logger_factory
from shared.aws_s3_util import (
    upload_json_objects,
    generate_unique_s3_key,
)
from shared.utils import parse_datetime_as_utc
from tools.property_store_tool import get_list_of_files
from crawler.redfin_spider.aws_s3_pipeline import get_s3_key_prefix_from_json

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

    object_map: Dict[str, List[Dict[str, Any]]] = {}

    for file_name in files:
        file_path = os.path.join(property_file_dir, file_name)
        logger.info(f"Uploading file {file_path} to S3.")

        # Read JSON objects from the file
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                json_object = json.loads(line)
                s3_path = get_s3_key_prefix_from_json(
                    json_object,
                    "property_raw_data_tool"
                )

                if s3_path not in object_map:
                    object_map[s3_path] = []

                object_map[s3_path].append(json_object)

    for key, items in object_map.items():
        s3_key = generate_unique_s3_key(
            prefix=key,
            extension="jsonl",
        )

        upload_json_objects(
            json_objects=items,
            bucket_name=s3_bucket_name,
            s3_key=s3_key,
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
    start_file = "redfin_properties_20251212_204516.jsonl"
    end_file = "redfin_properties_20251216_183632.jsonl"

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