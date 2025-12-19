#!/usr/bin/env python3
"""
Property Query Command Line Tool

A command line tool for querying properties from DynamoDB using DynamoDBPropertyService.
Supports querying by property ID or address.
"""

import argparse
import json
import logging
import sys

from shared.iproperty import IProperty
from shared.iproperty_address import IPropertyAddress, InvalidAddressError
import shared.logger_factory as logger_factory

from data_service.dynamodb_property_service import DynamoDBPropertyService


def format_property_summary(property_obj: IProperty) -> str:
    """Format property information as a summary using the property's __str__ method."""
    return f"Property Details:\n================\n{str(property_obj)}"


def format_property_json(property_obj: IProperty) -> str:
    """Format property information as JSON."""
    # Convert property to a dictionary representation
    property_dict = {
        "id": property_obj.id,
        "address": {
            "street_name": property_obj.address.street_name,
            "unit": property_obj.address.unit,
            "city": property_obj.address.city,
            "state": property_obj.address.state,
            "zip_code": property_obj.address.zip_code,
            "address_hash": property_obj.address.address_hash
        },
        "property_type": property_obj.property_type.value,
        "status": property_obj.status.value,
        "price": float(property_obj.price) if property_obj.price else None,
        "area": {
            "value": float(property_obj.area.value),
            "unit": property_obj.area.unit.value
        },
        "lot_area": {
            "value": float(property_obj.lot_area.value),
            "unit": property_obj.lot_area.unit.value
        } if property_obj.lot_area else None,
        "number_of_bedrooms": float(property_obj.number_of_bedrooms),
        "number_of_bathrooms": float(property_obj.number_of_bathrooms),
        "year_built": property_obj.year_built,
        "last_updated": property_obj.last_updated.isoformat(),
        "data_sources": [
            {
                "source_id": ds.source_id,
                "source_url": ds.source_url,
                "source_name": ds.source_name
            } for ds in property_obj.data_sources
        ],
        "history": [
            {
                "id": event.id,
                "datetime": event.datetime.isoformat(),
                "event_type": event.event_type.value,
                "description": event.description,
                "price": float(event.price) if event.price else None,
                "source": event.source,
                "source_id": event.source_id
            } for event in property_obj.history.history
        ]
    }
    return json.dumps(property_dict, indent=2)

def query_property_by_id(service: DynamoDBPropertyService, property_id: str) -> IProperty | None:
    """Query property by ID."""

    logger = logger_factory.get_logger(__name__)
    try:
        logger.info(f"Querying property by ID: {property_id}")
        return service.get_property_by_id(property_id)
    except Exception as e:
        logger.error(f"Error querying property by ID {property_id}: {e}")
        raise


def query_property_by_address(service: DynamoDBPropertyService, address_str: str) -> IProperty | None:
    """Query property by address."""

    logger = logger_factory.get_logger(__name__)
    try:
        logger.info(f"Querying property by address: {address_str}")
        address = IPropertyAddress(address_str)
        return service.get_property_by_address(address)
    except InvalidAddressError as e:
        logger.error(f"Invalid address format: {e}")
        raise
    except Exception as e:
        logger.error(f"Error querying property by address {address_str}: {e}")
        raise


def main() -> None:
    """Main function for the property query tool."""
    parser = argparse.ArgumentParser(
        description="Query properties from DynamoDB using DynamoDBPropertyService",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python property_query_tool.py --id "e329420e-4dc6-4d88-bfe7-222a268c82b9"
  python property_query_tool.py --address "7503 152nd Ave NE, Redmond, WA 98052"
  python property_query_tool.py --id "some-id" --table "my-properties" --region "us-east-1"
        """
    )

    # Query options (mutually exclusive)
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument(
        '--id',
        type=str,
        help='Property ID to query'
    )
    query_group.add_argument(
        '--address',
        type=str,
        help='Property address to query'
    )

    # DynamoDB configuration
    parser.add_argument(
        '--table',
        type=str,
        default='properties',
        help='DynamoDB table name (default: properties)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-west-2',
        help='AWS region (default: us-west-2)'
    )

    # Output format
    parser.add_argument(
        '--output-format',
        type=str,
        choices=['summary', 'json'],
        default='summary',
        help='Output format (default: summary)'
    )

    # Verbose logging
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Set up logging
    logger_factory.configure_logger(
        enable_file_logging = False,
        enable_console_logging= True,
        log_level=logging.INFO,
    )
    logger = logger_factory.get_logger(__name__)

    try:
        # Initialize DynamoDB service
        logger.info(f"Initializing DynamoDB service with table: {args.table}, region: {args.region}")
        service = DynamoDBPropertyService(args.table, args.region)

        # Query property
        property_obj = None
        if args.id:
            property_obj = query_property_by_id(service, args.id)
        elif args.address:
            property_obj = query_property_by_address(service, args.address)

        # Handle results
        if property_obj is None:
            query_type = "ID" if args.id else "address"
            query_value = args.id if args.id else args.address
            logger.info(f"Property not found for {query_type}: {query_value}")
            sys.exit(1)

        # Format and output results
        if args.output_format == 'json':
            output = format_property_json(property_obj)
        else:  # summary
            output = format_property_summary(property_obj)

        logger.info(output)

        # Close service
        service.close()

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
