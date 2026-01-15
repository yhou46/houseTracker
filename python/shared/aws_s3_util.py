"""
S3 utility functions for managing S3 buckets and operations.
"""

from typing import Optional, List, Dict, Any
import json
from datetime import datetime, timezone
import uuid

# boto3 imports
import boto3
from botocore.client import BaseClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import PutObjectRequestTypeDef
from botocore.exceptions import ClientError

from shared.logger_factory import configure_logger, get_logger
from shared.utils import parse_datetime_as_utc

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

def get_s3_key_from_json(json_object: Dict[str, Any], worker_id: str) -> str:
    """
    Generate S3 key using data source, date, and worker ID.

    Args:
        json_object: JSON object containing property data
        worker_id: Unique worker identifier

    Returns:
        S3 key in format: {data_source}/{date_str}/{worker_id}.jsonl
    """
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

    if data_source is None or date_str is None:
        raise ValueError(f"Failed to construct S3 key from JSON object. data_source: {data_source}, date_str: {date_str}")

    s3_key = f"{data_source}/{date_str}/{worker_id}.jsonl"
    return s3_key

# def generate_new_s3_key(old_key: str) -> str:
#     """
#     Generate a new S3 key by incrementing the suffix number in the key.

#     Handles keys like:
#     - redfin/20260114/worker_id.jsonl -> redfin/20260114/worker_id__1.jsonl
#     - redfin/20260114/worker_id__1.jsonl -> redfin/20260114/worker_id__2.jsonl

#     Args:
#         old_key: The original S3 key.

#     Returns:
#         A new S3 key with an incremented suffix.
#     """
#     if '.' not in old_key:
#         raise ValueError("Invalid S3 key format. Key must contain a file extension.")

#     # Split the key into base and extension
#     base, extension = old_key.rsplit('.', 1)

#     # Check if the base ends with __N (double underscore and numeric suffix)
#     if '__' in base:
#         base_parts = base.rsplit('__', 1)
#         if base_parts[-1].isdigit():
#             # Increment existing suffix
#             new_base = f"{base_parts[0]}__{int(base_parts[-1]) + 1}"
#         else:
#             # Has __ but not numeric suffix, add __1
#             new_base = f"{base}__1"
#     else:
#         # No suffix at all, add __1
#         new_base = f"{base}__1"

#     # Combine the new base with the original extension
#     return f"{new_base}.{extension}"

def generate_unique_s3_key(
    prefix: str,
    extension: str | None,
    ) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short_uuid = str(uuid.uuid4())[:8]

    if extension is not None and extension != "":
        return f"{prefix}_{timestamp}_{short_uuid}.{extension}"
    else:
        return f"{prefix}_{timestamp}_{short_uuid}"

def is_s3_key_exists(
        bucket_name: str,
        object_key: str,
        s3_client: S3Client,
        ) -> bool:
    """
    Checks if an S3 object exists.
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True # Key exists
    except ClientError as error:
        if error.response['Error']['Code'] == '404':
            # The object does not exist
            return False
        else:
            # Re-raise the exception if it's a different error (e.g., permissions)
            raise error

def upload_json_objects(
    json_objects: List[Dict[str, Any]],
    bucket_name: str,
    s3_key: str,
    region: str = "us-west-2",
    aws_profile: Optional[str] = None,
    overwrite_if_key_exists: bool = False,
) -> None:
    """
    Uploads a list of JSON objects to S3 as a JSONL file.

    Args:
        json_objects: List of dictionaries to upload as JSON objects.
        bucket_name: Name of the S3 bucket.
        s3_key: S3 key (path) for the object.
        region: AWS region (default: 'us-west-2').
        aws_profile: Optional AWS profile name to use for authentication.
        overwrite_if_key_exists: If False, raises error if key already exists; if True, overwrites existing key.

    Raises:
        ValueError: If s3_key already exists and overwrite_if_key_exists is False.
        ClientError: If there is an AWS service error during upload.
    """
    logger = get_logger(__name__)

    if len(json_objects) == 0:
        logger.info(f"No input objects. Skip the upload")
        return

    # Create S3 client with optional profile
    s3_client = get_aws_s3_client(region=region, aws_profile=aws_profile)

    # Convert JSON objects to JSONL format (one JSON object per line)
    jsonl_lines = [json.dumps(obj, ensure_ascii=False) for obj in json_objects]
    content = '\n'.join(jsonl_lines) + '\n'

    try:
        logger.info(f"Uploading {len(json_objects)} objects to s3://{bucket_name}/{s3_key}")

        # Use IfNoneMatch='*' to fail if key exists (when overwrite_if_key_exists is False)
        put_object_params: PutObjectRequestTypeDef = {
            'Bucket': bucket_name,
            'Key': s3_key,
            'Body': content.encode('utf-8'),
            'ContentType': 'application/x-jsonl'
        }

        if not overwrite_if_key_exists:
            put_object_params['IfNoneMatch'] = '*'

        s3_client.put_object(**put_object_params)
        logger.info(f"Successfully uploaded {len(json_objects)} objects to s3://{bucket_name}/{s3_key}")

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        error_message = e.response.get('Error', {}).get('Message', str(e))

        if error_code == 'PreconditionFailed':
            logger.error(f"S3 key '{s3_key}' already exists and overwrite_if_key_exists is False")
            raise ValueError(f"S3 key '{s3_key}' already exists") from e
        else:
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

    s3_key = "test/20260115/redfin_spider_monolith_20260114_200123_b0bb1d53_1.jsonl"

    s3_client = get_aws_s3_client()

    print(f"s3_key exist: {is_s3_key_exists(bucket_name, s3_key, s3_client)}")

    json_objects: List[Dict[str, Any]] = [
        {
            "abc": 12
        },
        {
            "bcd": "hello"
        }
    ]

    # json_objects: List[Dict[str, Any]] = [
    #     # Add test JSON objects here
    # ]
    upload_json_objects(
        json_objects=json_objects,
        bucket_name=bucket_name,
        region=region,
        s3_key=s3_key,
    )

