from dataclasses import dataclass, field
from typing import TypedDict
from datetime import datetime
import json
import os
import asyncio
from typing import (
    Dict,
    Any,
)

import redis.asyncio as redisAsync

from shared.async_service import AsyncService, run_async_service
from shared.redis_stream_util import (
    RedisConfig,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    RedisStreamTrimConfig,
    RedisStreamMessage,
    always_trim,
)
from shared.logger_factory import LoggerLike
from shared.config_util import get_config_from_file
import shared.logger_factory as logger_factory
from data_service.dynamodb_property_service import DynamoDBPropertyService
from data_service.redfin_data_parser import (
    parse_raw_data_to_property,
    PropertyDataStreamParsingError,
)
from data_service.iproperty_data_reader import RawPropertyData
from data_service.redfin_data_reader import get_raw_data_entry
from crawler.redfin_spider.pipelines import RawPropertyMessageData


@dataclass
class PropertyDataIngestionServiceConfig:
    """Configuration for PropertyDataIngestionService"""

    # Redis connection
    redis_config: RedisConfig

    # Consumer and trimmer configs (reuse existing classes)
    consumer_config: RedisStreamConsumerConfig
    trimmer_config: RedisStreamTrimConfig

    # DynamoDB settings
    dynamodb_table_name: str = "properties"
    dynamodb_region: str = "us-west-2"

    # Whether to shutdown the service when it is idle
    shutdown_when_idle_seconds: int | None = None

    # Placeholders for future implementation
    rate_limit_per_second: int | None = None  # Not implemented yet
    dead_letter_stream: str | None = None  # Not implemented yet


class IngestionMetrics(TypedDict):
    """Metrics for property data ingestion."""
    messages_received: int
    messages_processed: int
    messages_failed_parsing: int
    messages_failed_storage: int
    messages_failed_total: int
    started_at: datetime | None


class PropertyDataIngestionMessageHandler:
    """
    Message handler for processing raw property data from Redis stream.

    Implements the RedisStreamMessageHandler protocol (async callable).
    Flow:
    1. Deserialize Redis message → RawPropertyMessageData
    2. Parse JSON data → RawPropertyData
    3. Convert to IProperty using parse_raw_data_to_property()
    4. Store to DynamoDB via DynamoDBPropertyService
    """

    def __init__(
        self,
        property_service: DynamoDBPropertyService,
        logger: LoggerLike,
    ):
        self._property_service = property_service
        self._logger = logger
        self._metrics: IngestionMetrics = {
            "messages_received": 0,
            "messages_processed": 0,
            "messages_failed_parsing": 0,
            "messages_failed_storage": 0,
            "messages_failed_total": 0,
            "started_at": None,
        }

    @property
    def metrics(self) -> IngestionMetrics:
        """Get current metrics."""
        return self._metrics.copy()

    def start_tracking(self) -> None:
        """Start tracking metrics with current timestamp."""
        self._metrics["started_at"] = datetime.now()

    async def __call__(self, message: RedisStreamMessage) -> None:
        """
        Process a single Redis stream message.

        Args:
            message: RedisStreamMessage containing raw property data
        """
        message_id = message.redis_stream_message_id
        self._metrics["messages_received"] += 1

        try:
            # Step 1: Deserialize Redis message to RawPropertyMessageData
            raw_message = RawPropertyMessageData.from_redis_fields(message.data)

            # Step 2: Parse JSON data to dict, then to RawPropertyData
            json_data = json.loads(raw_message.data)
            raw_property_data = get_raw_data_entry(json_data)

            self._logger.info(
                f"Processing property: url={raw_message.url}, "
                f"redfin_id={raw_message.redfin_id}, "
                f"zip_code={raw_message.zip_code}"
            )

            # Step 3: Convert RawPropertyData to IProperty
            property_metadata, property_history = parse_raw_data_to_property(raw_property_data)

            self._logger.info(f"Parsed property: {property_metadata}, history: {property_history}")

            # Step 4: Store to DynamoDB
            try:
                stored_property = self._property_service.create_or_update_property(
                    property_metadata=property_metadata,
                    property_history=property_history,
                )

                self._logger.info(
                    f"Stored property: id={stored_property.id}, "
                    f"address={stored_property.metadata.address}, "
                    f"message_id={message_id}"
                )
                self._metrics["messages_processed"] += 1

            except Exception as e:
                self._metrics["messages_failed_storage"] += 1
                self._logger.error(
                    f"Storage error for message {message_id}: {e}",
                    exc_info=True
                )
                raise

        except PropertyDataStreamParsingError as e:
            # Log parsing errors but don't re-raise (message will be acknowledged)
            # TODO: Send to dead-letter stream when implemented
            self._metrics["messages_failed_parsing"] += 1
            self._metrics["messages_failed_total"] += 1
            self._logger.warning(
                f"Parsing error for message {message_id}: {e}, "
                f"error_code={e.error_code.value}, "
                f"input_data={e.input_data}"
            )

        except Exception as e:
            # Log unexpected errors
            self._logger.error(
                f"Unexpected error processing message {message_id}: {e}",
                exc_info=True
            )
            self._metrics["messages_failed_total"] += 1

            # TODO: Send to dead-letter stream when implemented


class PropertyDataIngestionService(AsyncService):
    """
    Service that ingests raw property data from Redis stream and stores to DynamoDB.

    Composes:
    - RedisStreamConsumer: Consumes messages from Redis stream
    - PropertyDataIngestionMessageHandler: Processes each message
    - DynamoDBPropertyService: Stores properties to DynamoDB
    """

    def __init__(self, config: PropertyDataIngestionServiceConfig):
        self._config = config
        self._logger = logger_factory.get_logger(f"{__name__}.{self.__class__.__name__}")

        # Components (initialized in start())
        self._redis_client: redisAsync.Redis | None = None
        self._consumer: RedisStreamConsumer | None = None
        self._property_service: DynamoDBPropertyService | None = None
        self._message_handler: PropertyDataIngestionMessageHandler | None = None

        self._running = False
        self._shutdown_when_idle_seconds: int | None = None
        self._monitor_consumer_status_interval_seconds = 5
        self._monitor_idle_task: asyncio.Task[None] | None = None

    """
    ===========================================
    Public methods inherited from base
    ===========================================
    """
    async def start(self) -> None:
        """Initialize components and start the consumer."""
        if self._running:
            self._logger.warning("Service already running")
            return

        self._logger.info("Starting PropertyDataIngestionService...")

        # Initialize Redis client
        self._redis_client = redisAsync.Redis(
            host=self._config.redis_config.host,
            port=self._config.redis_config.port,
            password=self._config.redis_config.password,
            decode_responses=self._config.redis_config.decode_responses,
            socket_connect_timeout=self._config.redis_config.socket_connect_timeout,
        )

        # Initialize DynamoDB property service
        self._property_service = DynamoDBPropertyService(
            table_name=self._config.dynamodb_table_name,
            region_name=self._config.dynamodb_region,
        )

        # Initialize message handler
        self._message_handler = PropertyDataIngestionMessageHandler(
            property_service=self._property_service,
            logger=self._logger,
        )
        self._message_handler.start_tracking()

        # Overwrite consumer's shutdown idle seconds
        self._shutdown_when_idle_seconds = self._config.shutdown_when_idle_seconds
        self._config.consumer_config.shutdown_when_idle_seconds = self._shutdown_when_idle_seconds

        # Initialize consumer
        self._consumer = RedisStreamConsumer(
            redis_client=self._redis_client,
            consumer_config=self._config.consumer_config,
            trimmer_config=self._config.trimmer_config,
            trim_trigger=always_trim,
            message_handler=self._message_handler,
        )

        # Start consumer
        await self._consumer.start()
        self._running = True

        # Start monitor task to check if consumer stops
        if self._shutdown_when_idle_seconds is not None and self._shutdown_when_idle_seconds > 0:
            self._monitor_idle_task = asyncio.create_task(self._monitor_consumer_status())

        self._logger.info(
            f"PropertyDataIngestionService started: "
            f"stream={self._config.consumer_config.stream_name}, "
            f"group={self._config.consumer_config.consumer_group}"
        )

    async def stop(self) -> None:
        """Gracefully shutdown the service."""
        self._logger.info("Stopping PropertyDataIngestionService...")

        # Cancel monitor task
        if self._monitor_idle_task:
            self._monitor_idle_task.cancel()
            try:
                await self._monitor_idle_task
            except asyncio.CancelledError:
                pass

        # Stop consumer
        if self._consumer:
            await self._consumer.stop()

        # Close Redis connection
        if self._redis_client:
            await self._redis_client.aclose()

        # Log final metrics
        if self._message_handler:
            metrics = self._message_handler.metrics
            self._logger.info(
                f"Ingestion metrics: "
                f"received={metrics['messages_received']}, "
                f"processed={metrics['messages_processed']}, "
                f"failed_parsing={metrics['messages_failed_parsing']}, "
                f"failed_storage={metrics['messages_failed_storage']}, "
                f"started_at={metrics['started_at']}"
            )

        self._running = False
        self._logger.info("PropertyDataIngestionService stopped")

    def is_running(self) -> bool:
        return self._running

    def get_metrics(self) -> IngestionMetrics | None:
        """Get current ingestion metrics."""
        if self._message_handler:
            return self._message_handler.metrics
        return None

    """
    ===========================================
    Private methods
    ===========================================
    """
    async def _monitor_consumer_status(self) -> None:
        """Monitor consumer status and stop service if consumer stops."""

        while self._running:
            try:
                await asyncio.sleep(self._monitor_consumer_status_interval_seconds)

                if self._consumer and not self._consumer.is_running():
                    self._logger.info(
                        "Consumer has stopped. Initiating service shutdown..."
                    )
                    # Trigger stop without awaiting to avoid blocking the monitor
                    asyncio.create_task(self.stop())
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"Error in consumer monitor: {e}", exc_info=True)

        self._logger.debug("Consumer monitor stopped")


def get_service_config(config_data: Dict[str, Any]) -> PropertyDataIngestionServiceConfig:
    """Load service configuration from a JSON config file."""

    redis_section = config_data["redis"]
    redis_config = RedisConfig(
        host=redis_section["host"],
        port=redis_section["port"],
        password=redis_section.get("password"),
    )

    stream_section = config_data["stream"]
    consumer_section = config_data["consumer_settings"]
    trimmer_section = config_data["trimmer_settings"]

    stream_name = stream_section["stream_name"]

    consumer_config = RedisStreamConsumerConfig(
        stream_name=stream_name,
        consumer_group=stream_section["consumer_group"],
        consumer_name_prefix=stream_section["consumer_name_prefix"],
        read_block_ms=consumer_section["read_block_ms"],
        read_batch_size=consumer_section["read_batch_size"],
        read_delay_ms=consumer_section.get("read_delay_ms"),
        claim_interval_seconds=consumer_section["claim_interval_seconds"],
        claim_idle_ms=consumer_section["claim_idle_ms"],
        claim_count=consumer_section["claim_count"],
        processing_timeout_seconds=consumer_section["processing_timeout_seconds"],
        max_retries=consumer_section["max_retries"],
        shutdown_grace_period_seconds=consumer_section["shutdown_grace_period_seconds"],
        shutdown_when_idle_seconds=consumer_section.get("shutdown_when_idle_seconds"),
    )

    trimmer_config = RedisStreamTrimConfig(
        stream_name=stream_name,
        trim_interval_seconds=trimmer_section["trim_interval_seconds"],
        trim_max_len=trimmer_section["trim_max_len"],
        trim_approximate=trimmer_section.get("trim_approximate", True),
    )

    dynamodb_section = config_data["dynamodb"]

    return PropertyDataIngestionServiceConfig(
        redis_config=redis_config,
        consumer_config=consumer_config,
        trimmer_config=trimmer_config,
        dynamodb_table_name=dynamodb_section["table_name"],
        dynamodb_region=dynamodb_section["region"],
        shutdown_when_idle_seconds=config_data["shutdown_when_idle_seconds"],
    )


def get_default_config_path() -> str:
    """Get the default config file path relative to this module."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "config")


async def main(config_path: str | None = None) -> int:
    """Main entry point for the PropertyDataIngestionService."""

    # Configure logger
    log_file_dir = os.path.join(os.path.dirname(__file__), "property_data_ingestion_logs")
    logger_factory.configure_logger(
        log_file_prefix="property_ingestion_service",
        log_file_path=log_file_dir,
        enable_console_logging=True,
        enable_file_logging=True,
    )

    logger = logger_factory.get_logger(__name__)

    # Load config from file
    if config_path is None:
        config_path = get_default_config_path()

    logger.info(f"Loading config from: {config_path}")
    config_data = get_config_from_file(
        config_file_path=config_path,
        config_file_prefix="property_data_ingestion_service",
    )
    service_config = get_service_config(config_data)

    logger.info("Initializing PropertyDataIngestionService...")

    # Create service
    service = PropertyDataIngestionService(service_config)

    # Run service with signal handling
    exit_code = await run_async_service(
        start_handler=service.start,
        shutdown_handler=service.stop,
    )

    return exit_code

# TODO: add shared arg parsing
if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
