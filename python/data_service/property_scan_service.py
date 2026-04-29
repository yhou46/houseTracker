from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any
import asyncio
import os

import redis.asyncio as redisAsync

import shared.logger_factory as logger_factory
from shared.config_util import get_config_from_file
from shared.redis_stream_util import (
    RedisConfig,
    RedisStreamProducerConfig,
    RedisStreamProducer,
)
from shared.iproperty import PropertyStatus
from data_service.dynamodb_property_service import DynamoDBPropertyService
from data_service.iproperty_service import PropertyQueryPattern
from crawler.redfin_spider.pipelines import PropertyUrlMessageData


@dataclass
class PropertyScanServiceConfig:
    redis_config: RedisConfig
    stream_name: str
    dynamodb_table_name: str
    dynamodb_region: str
    scan_queries: List[PropertyQueryPattern]


class PropertyScanService:

    def __init__(self, config: PropertyScanServiceConfig) -> None:
        self._config = config
        self._logger = logger_factory.get_logger(f"{__name__}.{self.__class__.__name__}")

    async def scan(self) -> None:
        redis_client = redisAsync.Redis(
            host=self._config.redis_config.host,
            port=self._config.redis_config.port,
            password=self._config.redis_config.password,
            decode_responses=True,
            socket_connect_timeout=5,
        )

        producer = RedisStreamProducer(
            redis_client=redis_client,
            config=RedisStreamProducerConfig(stream_name=self._config.stream_name),
        )

        dynamodb_service = DynamoDBPropertyService(
            table_name=self._config.dynamodb_table_name,
            region_name=self._config.dynamodb_region,
        )

        try:
            total_published = 0
            for query in self._config.scan_queries:
                self._logger.info(f"Starting scan for query: {query}")
                query_published = 0
                last_evaluated_key = None

                while True:
                    properties, last_evaluated_key = dynamodb_service.query_properties(
                        query,
                        exclusive_start_key=last_evaluated_key,
                    )

                    messages = []
                    for property in properties:
                        if not property.data_sources:
                            self._logger.warning(f"Property {property.id} has no data sources, skipping")
                            continue

                        message = PropertyUrlMessageData(
                            property_url=property.data_sources[0].source_url,
                            scraped_at_utc=datetime.now(timezone.utc).isoformat(),
                            data_source=property.data_sources[0].source_name,
                            from_page_url=property.data_sources[0].source_url,
                            property_id=property.id,
                        )
                        messages.append(message.to_redis_fields())

                    if messages:
                        await producer.publish_batch(messages)
                        query_published += len(messages)
                        self._logger.info(f"Published {query_published} URLs so far for query: {query}")

                    if not last_evaluated_key:
                        break

                self._logger.info(f"Finished scan for query: {query}, published: {query_published}")
                total_published += query_published

            self._logger.info(f"Scan complete. Total URLs published: {total_published}")

        finally:
            await redis_client.aclose()


def get_service_config(config_data: Dict[str, Any]) -> PropertyScanServiceConfig:
    redis_section = config_data["redis"]
    redis_config = RedisConfig(
        host=redis_section["host"],
        port=redis_section["port"],
        password=redis_section.get("password"),
    )

    stream_name = config_data["property_url_stream"]["stream_name"]

    dynamodb_section = config_data["dynamodb"]

    status_map = {s.value: s for s in PropertyStatus}
    scan_queries = []
    for q in config_data["scan_queries"]:
        status_str = q["status"]
        status = status_map.get(status_str)
        if status is None:
            raise ValueError(f"Unknown status in scan_queries config: '{status_str}'")
        scan_queries.append(PropertyQueryPattern(
            state=q["state"],
            status_list=[status],
        ))

    return PropertyScanServiceConfig(
        redis_config=redis_config,
        stream_name=stream_name,
        dynamodb_table_name=dynamodb_section["table_name"],
        dynamodb_region=dynamodb_section["region"],
        scan_queries=scan_queries,
    )


async def main(config_path: str | None = None) -> None:
    log_file_dir = os.path.join(os.path.dirname(__file__), "property_scan_logs")
    logger_factory.configure_logger(
        log_file_prefix="property_scan_service",
        log_file_path=log_file_dir,
        enable_console_logging=True,
        enable_file_logging=True,
    )

    logger = logger_factory.get_logger(__name__)

    if config_path is None:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")

    logger.info(f"Loading config from: {config_path}")
    config_data = get_config_from_file(
        config_file_path=config_path,
        config_file_prefix="property_scan_service",
    )
    config = get_service_config(config_data)

    service = PropertyScanService(config)
    await service.scan()


if __name__ == "__main__":
    asyncio.run(main())
