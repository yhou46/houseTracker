"""
S3 utility functions for managing S3 buckets and operations.
"""

import boto3
from botocore.client import BaseClient
from mypy_boto3_s3 import S3Client
from botocore.exceptions import ClientError
from typing import Optional, List, Dict, Any
import json

from shared.logger_factory import configure_logger, get_logger
from data_service.redfin_data_parser import parse_datetime_as_utc

def get_aws_s3_client(
        region: str = "us-west-2",
        aws_profile: Optional[str] = None,
        ) -> S3Client:
    if aws_profile:
        session = boto3.Session(profile_name=aws_profile)
        return session.client("s3", region_name=region)
    else:
        return boto3.client("s3", region_name=region)

# Never tested
def create_s3_bucket(
    bucket_name: str,
    region: str = "us-west-2",
    aws_profile: Optional[str] = None
) -> bool:
    """
    Create a new S3 bucket in the specified region.

    Args:
        bucket_name: Name of the bucket to create (must be globally unique)
        region: AWS region where the bucket will be created (default: us-west-2)
        aws_profile: AWS profile name to use (optional, uses default if not specified)

    Returns:
        True if bucket was created successfully, False otherwise

    Raises:
        ClientError: If there's an AWS service error
    """

    logger = get_logger(__name__)
    try:
        # Create S3 client with optional profile
        s3_client = get_aws_s3_client(region=region, aws_profile=aws_profile)

        # For us-east-1, LocationConstraint is not needed
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            # Type ignore needed: boto3 expects literal types but we accept string parameter
            bucket_config = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=bucket_config  # type: ignore[arg-type]
            )

        logger.info(f"Bucket '{bucket_name}' created successfully in region '{region}'")
        return True

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        error_message = e.response.get('Error', {}).get('Message', str(e))

        if error_code == 'BucketAlreadyExists':
            logger.warning(f"Bucket '{bucket_name}' already exists")
            return False
        elif error_code == 'BucketAlreadyOwnedByYou':
            logger.info(f"Bucket '{bucket_name}' already owned by you")
            return True  # Consider this a success since we own it
        else:
            logger.error(f"Error creating bucket '{bucket_name}': {error_code} - {error_message}")
            raise


def bucket_exists(
    bucket_name: str,
    region: str = "us-west-2",
    aws_profile: Optional[str] = None
) -> bool:
    """
    Check if an S3 bucket exists and is accessible.

    Args:
        bucket_name: Name of the bucket to check
        region: AWS region where the bucket should be located (default: us-west-2)
        aws_profile: AWS profile name to use (optional, uses default if not specified)

    Returns:
        True if bucket exists and is accessible, False otherwise
    """

    logger = get_logger(__name__)
    try:
        # Create S3 client with optional profile
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            s3_client = session.client('s3', region_name=region)
        else:
            s3_client = boto3.client('s3', region_name=region)

        # Use head_bucket to check if bucket exists (doesn't require list permissions)
        s3_client.head_bucket(Bucket=bucket_name)
        logger.debug(f"Bucket '{bucket_name}' exists and is accessible")
        return True

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')

        if error_code == '404':
            logger.info(f"Bucket '{bucket_name}' does not exist")
            return False
        elif error_code == '403':
            logger.info(f"Bucket '{bucket_name}' exists but access is forbidden")
            # Bucket exists but we don't have permission - return True since it exists
            return True
        else:
            logger.error(f"Error checking bucket '{bucket_name}': {error_code}")
            # For other errors, assume bucket doesn't exist
            return False

    except Exception as e:
        logger.error(f"Unexpected error checking bucket '{bucket_name}': {e}")
        return False

# Never tested
def ensure_bucket_exists(
    bucket_name: str,
    region: str = "us-west-2",
    aws_profile: Optional[str] = None
) -> bool:
    """
    Ensure a bucket exists, creating it if it doesn't.

    Args:
        bucket_name: Name of the bucket
        region: AWS region where the bucket should be located (default: us-west-2)
        aws_profile: AWS profile name to use (optional, uses default if not specified)

    Returns:
        True if bucket exists (was already there or created), False otherwise
    """
    logger = get_logger(__name__)
    # Check if bucket already exists
    if bucket_exists(bucket_name, region, aws_profile):
        logger.info(f"Bucket '{bucket_name}' already exists")
        return True

    # Create bucket if it doesn't exist
    logger.info(f"Bucket '{bucket_name}' does not exist, creating it...")
    return create_s3_bucket(bucket_name, region, aws_profile)

def get_s3_key_from_json(json_object: Dict[str, Any]) -> str:
    redfin_key = "redfinId"
    is_redfin = redfin_key in json_object
    data_source = None

    if is_redfin:
        data_source = 'redfin'
    else:
        raise ValueError(f"Failed to determine the data source from JSON object. Data source key: {redfin_key} not found.")

    scraped_at = json_object.get("scrapedAt", None)
    date_str = None
    if scraped_at != None:
        date_str = parse_datetime_as_utc(scraped_at).strftime("%Y%m%d")
    else:
        raise ValueError("scrapedAt field is missing in JSON object.")

    zip_code = json_object.get("zipCode", None)

    if data_source is None or date_str is None or zip_code is None:
        raise ValueError(f"Failed to construct S3 key from JSON object. data_source: {data_source}, date_str: {date_str}, zip_code: {zip_code}")

    s3_key = f"{data_source}/{date_str}/{zip_code}/{data_source}_{date_str}_{zip_code}.jsonl"
    return s3_key

def generate_new_s3_key(old_key: str) -> str:
    """
    Generate a new S3 key by incrementing the suffix number in the key.

    Args:
        old_key: The original S3 key.

    Returns:
        A new S3 key with an incremented suffix.
    """
    if '.' not in old_key:
        raise ValueError("Invalid S3 key format. Key must contain a file extension.")

    # Split the key into base and extension
    base, extension = old_key.rsplit('.', 1)

    # Split the base into parts once and cache the result
    parts = base.split('_')

    # Check if the last part is a numeric suffix
    if parts[-1].isdigit() and len(parts) == 4 :
        parts[-1] = str(int(parts[-1]) + 1)  # Increment the numeric suffix
    else:
        parts.append('1')  # Add the initial suffix

    # Combine the parts back into the base
    new_base = '_'.join(parts)

    # Combine the new base with the original extension
    return f"{new_base}.{extension}"

def upload_json_objects(
    json_objects: List[Dict[str, Any]],
    bucket_name: str,
    region: str = "us-west-2",
    aws_profile: Optional[str] = None,
    continue_if_key_exists: bool = True,
) -> None:
    """
    Uploads a list of JSON objects to S3, handling key collisions by generating new keys if needed.

    This function stores each JSON object as a line in a JSONL file in S3. For each unique S3 key (derived from the object),
    it checks if the key already exists in the bucket:
      1. If the key does not exist, it creates a new file and uploads the objects.
      2. If the key exists, it can either skip uploading (if continue_if_key_exists is False),
         or generate a new key with an incremented suffix and upload the objects there.

    Note: S3 does not support true append operations. This function does not merge with existing content; it only creates new files or new versions with incremented keys.

    Args:
        json_objects: List of dictionaries to upload as JSON objects.
        bucket_name: Name of the S3 bucket.
        region: AWS region (default: 'us-west-2').
        aws_profile: Optional AWS profile name to use for authentication.
        continue_if_key_exists: If True, generates a new key if the original exists; if False, skips upload for existing keys.

    Raises:
        ClientError: If there is an AWS service error during upload.
        ValueError: If json_objects is empty.
    """
    logger = get_logger(__name__)
    s3_key_object_map: Dict[str, List[str]] = {}

    # Create jsonl based on s3 key
    for object in json_objects:
        key = get_s3_key_from_json(object)
        if key not in s3_key_object_map:
            s3_key_object_map[key] = []
        s3_key_object_map[key].append(json.dumps(object, ensure_ascii=False))

    logger.info(f"Number of unique S3 keys to upload: {len(s3_key_object_map)}")


    # Create S3 client with optional profile
    s3_client = get_aws_s3_client(region=region, aws_profile=aws_profile)

    for key, object_strings in s3_key_object_map.items():
        key_exists = False
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            existing_content = response['Body'].read().decode('utf-8')
            key_exists = True
            logger.info(f"existing content: {existing_content}")
            logger.info(f"S3 key '{key}' exists, will create new key")

            if not continue_if_key_exists:
                logger.warning(f"S3 key '{key}' already exists and continue_if_key_exists is False. Skipping upload.")
                continue
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'NoSuchKey':
                logger.info(f"S3 key '{key}' does not exist")
            else:
                # Re-raise if it's a different error (e.g., access denied)
                raise

        try:
            s3_key = key if not key_exists else generate_new_s3_key(key)

            content = '\n'.join(object_strings) + '\n'

            # Upload combined content
            logger.info(f"Uploading {len(object_strings)} objects to s3://{bucket_name}/{s3_key}")
            content_type: str = "application/x-jsonl"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=content.encode('utf-8'),
                ContentType=content_type
            )
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            logger.error(f"Error uploading JSON objects to s3://{bucket_name}/{s3_key}: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading JSON objects to s3://{bucket_name}/{s3_key}: {e}")
            raise


if __name__ == "__main__":

    configure_logger(
        enable_console_logging=True,
    )

    bucket_name = "myhousetracker-99ce79"
    region = "us-west-2"

    json_objects: List[Dict[str, Any]] = [
        # Add test JSON objects here
    ]
    upload_json_objects(
        json_objects=json_objects,
        bucket_name=bucket_name,
        region=region
    )

