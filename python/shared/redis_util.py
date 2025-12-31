from typing import (
    Any,
    Callable,
    Awaitable,
    List,
    Dict,
    TypedDict,
    TypeAlias,
    Tuple,
)
from dataclasses import dataclass

import datetime
import uuid
import asyncio

import redis
import redis.asyncio as redisAsync
from redis.exceptions import ResponseError

import shared.logger_factory as logger_factory
from shared.async_service import AsyncService

"""
Redis client requirements:

Redis stream producer:
async Redis client that can produce messages to a Redis stream

Redis stream consumer:
- async Redis client that can consume messages from a Redis stream, potentially using consumer groups.
- It should remove consumed messages from the stream.
- It should get pending messages (message delivered but not processed yet) from other consumers in the same consumer group.

"""

class RedisConfig:
    def __init__(
            self,
            host: str, port: int,
            password: str | None,
            decode_responses: bool = True,
            socket_connect_timeout: int = 5,
            ):
        self.host = host
        self.port = port
        self.password = password

        # Whether to decode response to string from bytecode
        self.decode_responses = decode_responses

        self.socket_connect_timeout = socket_connect_timeout

@dataclass
class RedisStreamTrimConfig:
    stream_name: str
    trim_interval_seconds: int
    trim_max_len: int
    trim_approximate: bool = True

class RedisClient:
    """
    Synchronous Redis client
    """

    def __init__(self, config: RedisConfig):
        self.client = redis.Redis(
            host=config.host,
            port=config.port,
            password=config.password,
            decode_responses=config.decode_responses,
            socket_connect_timeout=config.socket_connect_timeout,
        )

        self.defaultKeyExpiration = datetime.timedelta(days=1)

    def setKey(self, key: str, value: Any, expiration: datetime.timedelta | None = None) -> None:
        if expiration is None:
            expiration = self.defaultKeyExpiration
        self.client.setex(key, expiration, value)

    def getKey(self, key: str) -> Any:
        return self.client.get(key)

    def deleteKey(self, key: str) -> None:
        self.client.delete(key)


# TODO: add Redis client manager to create and shutdown redis clients

type RedisStreamTrimTriggerFunction = Callable[..., Awaitable[bool]]

async def always_trim() -> bool:
    """Always trigger trimming, used for testing"""
    return True

class RedisStreamTrimmer(AsyncService):
    """Handles periodic stream trimming with randomization"""

    def __init__(
            self,
            redis_client: redisAsync.Redis,
            config: RedisStreamTrimConfig,
            trim_trigger: RedisStreamTrimTriggerFunction,
            ):
        self._redis_client = redis_client
        self.config = config
        self._running = False
        self._task: asyncio.Task[None] | None = None

        self.logger = logger_factory.get_logger(__name__)
        self.trim_trigger = trim_trigger

    """
    Public
    """
    async def start(self) -> None:
        """Start the trimming task"""
        if self._running:
            self.logger.warning("Trimmer already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._trim_loop())
        self.logger.info(
            f"Started stream trimmer: interval={self.config.trim_interval_seconds}s, "
            f"maxlen={self.config.trim_max_len}"
        )

    async def wait_until_stopped(self) -> None:
        """Wait until the trimmer is stopped"""
        if self._running and self._task:
            await self._task

    async def stop(self) -> None:
        """Stop the trimming task"""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError as error:
                self.logger.info("Trimmer task cancelled", exc_info=True)
                pass

        self.logger.info("Stopped stream trimmer")

    def is_running(self) -> bool:
        return self._running

    async def _trim_loop(self) -> None:
        """
        Main trimming loop
        """
        while self._running:
            try:
                should_trim = await self.trim_trigger()

                if should_trim:
                    await self._trim_stream()
                else:
                    self.logger.debug("Skipped trim (randomization)")

                await asyncio.sleep(self.config.trim_interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in trim loop: {e}", exc_info=True)

    async def _trim_stream(self) -> None:
        """Execute XTRIM on the stream"""
        try:
            start_time = asyncio.get_event_loop().time()

            # Get stream length before trim
            length_before = await self._redis_client.xlen(self.config.stream_name)

            # Execute trim
            if self.config.trim_approximate:
                # Approximate trimming (faster, ~ operator)
                result = await self._redis_client.xtrim(
                    self.config.stream_name,
                    maxlen=self.config.trim_max_len,
                    approximate=True
                )
            else:
                # Exact trimming
                result = await self._redis_client.xtrim(
                    self.config.stream_name,
                    maxlen=self.config.trim_max_len,
                    approximate=False
                )

            # Get stream length after trim
            length_after = await self._redis_client.xlen(self.config.stream_name)

            elapsed = asyncio.get_event_loop().time() - start_time

            self.logger.info(
                f"Trimmed stream '{self.config.stream_name}': "
                f"before={length_before}, after={length_after}, "
                f"removed={result}, elapsed={elapsed:.3f}s"
            )

        except Exception as e:
            self.logger.error(f"Failed to trim stream: {e}", exc_info=True)

    async def force_trim(self) -> None:
        """Force an immediate trim (for testing/manual trigger)"""
        self.logger.info("Forcing stream trim")
        await self._trim_stream()

@dataclass
class RedisStreamConsumerConfig:

    """Consumer configuration"""
    # Stream and group settings
    stream_name: str
    consumer_group: str

    # Consumer identity
    consumer_name_prefix: str

    # Reading settings
    block_ms: int = 5000  # 5 seconds
    count: int = 10  # Batch size

    # Claiming settings
    claim_interval_seconds: int = 15
    claim_idle_ms: int = 60000  # claim pending messages older than 1 minute
    claim_count: int = 10

    # Processing settings
    processing_timeout_seconds: int = 45
    max_retries: int = 3

    # Graceful shutdown
    shutdown_grace_period_seconds: int = 30

# TODO: define Redis stream message type?

StreamMessage: TypeAlias = Tuple[str, Any] # (message_id, data)
StreamMessages: TypeAlias = List[StreamMessage]
XReadGroupResponseResp2: TypeAlias = List[Tuple[str, StreamMessages]] # each tuple is (stream_name, [messages]), it is actually a List but use Tuple for better type hinting

def validate_xreadgroup_response(response: Any) -> bool:
    """Validate the structure of XREADGROUP response"""

    logger = logger_factory.get_logger(__name__)
    if not isinstance(response, list):
        logger.error(f"XREADGROUP response is not a list: {response}")
        return False

    for stream_entry in response:
        if (not isinstance(stream_entry, list) and not isinstance(stream_entry, tuple)) or len(stream_entry) != 2:
            logger.error(f"stream_entry is not a list of length 2: {stream_entry}")
            return False
        stream_name, messages = stream_entry
        if not isinstance(stream_name, str):
            logger.error(f"stream_name is not str: {stream_name}")
            return False
        if not isinstance(messages, list):
            logger.error(f"messages is not list: {messages}")
            return False
        for message in messages:
            if not isinstance(message, tuple) or len(message) != 2:
                logger.error(f"message is not a tuple of length 2: {message}")
                return False
            message_id, data = message
            if not isinstance(message_id, str):
                logger.error(f"message_id is not str: {message_id}")
                return False

    return True

@dataclass
class RedisStreamMessage():
    stream_name: str
    redis_stream_message_id: str
    data: Any

def convert_xreadgroup_response(response: XReadGroupResponseResp2) -> List[RedisStreamMessage]:
    messages: List[RedisStreamMessage] = []

    for stream_name, stream_messages in response:
        # stream_name = stream_name_bytes.decode('utf-8')
        for message_id, data in stream_messages:
            messages.append(
                RedisStreamMessage(
                    stream_name=stream_name,
                    redis_stream_message_id=message_id,
                    data=data
                )
            )

    return messages


# Consumer Metrics
# TODO: should we rename it or remove it?
ConsumerMetrics = TypedDict(
    "ConsumerMetrics",
    {
        "messages_processed": int,
        "messages_claimed": int,
        "messages_failed": int,
        "started_at": datetime.datetime | None,
        "stream_length": int | None,
        "pending_count": int | None,
    }
)

# Type alias for message handler
RedisStreamMessageHandler: TypeAlias = Callable[[RedisStreamMessage], Awaitable[bool]]
class RedisStreamConsumer(AsyncService):
    """Async Redis Stream Consumer with consumer group support"""

    def __init__(
        self,
        consumer_config: RedisStreamConsumerConfig,
        trimmer_config: RedisStreamTrimConfig,
        trim_trigger: RedisStreamTrimTriggerFunction,
        redis_client: redisAsync.Redis,
        message_handler: RedisStreamMessageHandler
    ):
        self.consumer_config = consumer_config
        self.trimmer_config = trimmer_config
        self._redis_client = redis_client
        self.message_handler = message_handler

        self.logger = logger_factory.get_logger(__name__)

        # Redis client (will be initialized in start())
        # self.redis: Optional[Redis] = None

        # Consumer identity
        self.consumer_name = self._generate_consumer_name()

        # State management
        self._running = False
        # self._shutdown_event = asyncio.Event()
        self._tasks: List[asyncio.Task[None]] = []

        # Stream trimmer
        self.trimmer = RedisStreamTrimmer(
            redis_client,
            self.trimmer_config,
            trim_trigger,
        )

        self.metrics: ConsumerMetrics = {
            "messages_processed": 0,
            "messages_claimed": 0,
            "messages_failed": 0,
            "started_at": None,
            "stream_length": None,
            "pending_count": None,
        }

    """
    Public methods
    """
    async def start(self) -> None:
        """Start the consumer"""
        if self._running:
            self.logger.warning("Consumer already running")
            return

        self.logger.info(f"Starting consumer: {self.consumer_name}")

        # Ensure consumer group exists
        await self._ensure_consumer_group()

        # Setup signal handlers for graceful shutdown
        # self._setup_signal_handlers()

        # Start tasks
        self._running = True
        self.metrics["started_at"] = datetime.datetime.now(datetime.UTC)

        # Start trimmer
        await self.trimmer.start()

        # Create consumer tasks
        reader_task = asyncio.create_task(self._read_new_messages())
        claimer_task = asyncio.create_task(self._claim_pending_messages())

        self._tasks = [reader_task, claimer_task]
        # self._tasks = [reader_task]

        self.logger.info(
            f"Consumer started: stream={self.consumer_config.stream_name}, "
            f"group={self.consumer_config.consumer_group}, "
            f"consumer={self.consumer_name}"
        )

    async def stop(self) -> None:
        """Graceful shutdown"""
        self.logger.info("Shutting down consumer...")
        self._running = False

        # Stop trimmer
        if self.trimmer:
            await self.trimmer.stop()

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

        # Wait for tasks to complete with timeout
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=self.consumer_config.shutdown_grace_period_seconds
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some tasks did not complete within grace period")

        # Log final metrics
        self.logger.info(
            f"Consumer stopped. Metrics: "
            f"processed={self.metrics['messages_processed']}, "
            f"claimed={self.metrics['messages_claimed']}, "
            f"failed={self.metrics['messages_failed']}"
        )

    def is_running(self) -> bool:
        return self._running

    """
    Private methods
    """
    def _generate_consumer_name(self) -> str:
        """Generate unique consumer name for this instance"""
        timestamp = int(datetime.datetime.now(datetime.UTC).timestamp())
        unique_id = str(uuid.uuid4())[:8]

        return f"{self.consumer_config.consumer_name_prefix}-{timestamp}-{unique_id}"

    async def _ensure_consumer_group(self) -> None:
        """Ensure consumer group exists, create if not"""
        try:
            # Try to create consumer group starting from beginning (0)
            # Use '0' to process all messages, or '$' for only new messages
            await self._redis_client.xgroup_create(
                name=self.consumer_config.stream_name,
                groupname=self.consumer_config.consumer_group,
                id='0',  # Start from beginning
                mkstream=True  # Create stream if doesn't exist
            )
            self.logger.info(f"Created consumer group: {self.consumer_config.consumer_group}")
        except ResponseError as error:
            self.logger.error(f"ResponseError when creating consumer group: {error}")
            if "BUSYGROUP" in str(error):
                self.logger.info(f"Consumer group already exists: {self.consumer_config.consumer_group}")
            else:
                raise

    async def _read_new_messages(self) -> None:
        """Main loop: read new messages from stream"""
        self.logger.info("Started reading new messages")

        while self._running:
            try:
                # Read new messages using XREADGROUP
                # '>' means only new messages that haven't been delivered to any consumer
                response = await self._redis_client.xreadgroup(
                    groupname=self.consumer_config.consumer_group,
                    consumername=self.consumer_name,
                    streams={self.consumer_config.stream_name: '>'},
                    count=self.consumer_config.count,
                    block=self.consumer_config.block_ms
                )

                self.logger.info(
                    f"Received response:\n{response}"
                )

                if response:
                    await self._process_response(response, is_claimed=False)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error reading messages: {e}", exc_info=True)
                await asyncio.sleep(1)  # Brief pause before retry

        self.logger.info("Stopped reading new messages")

    async def _claim_pending_messages(self) -> None:
        """Periodic task: claim pending messages from other consumers"""
        self.logger.info("Started claiming pending messages")

        while self._running:
            try:
                await asyncio.sleep(self.consumer_config.claim_interval_seconds)

                # Use XAUTOCLAIM to claim old pending messages
                # Returns: [next_id, claimed_messages, deleted_message_ids]
                response = await self._redis_client.xautoclaim(
                    name=self.consumer_config.stream_name,
                    groupname=self.consumer_config.consumer_group,
                    consumername=self.consumer_name,
                    min_idle_time=self.consumer_config.claim_idle_ms,
                    start_id='0-0',  # Start from beginning
                    count=self.consumer_config.claim_count
                )

                # Result format: (next_id, claimed_messages)
                self.logger.info(f"XAUTOCLAIM response: {response}")
                next_id, claimed_messages = response[0], response[1]

                if claimed_messages:
                    self.logger.info(f"Claimed {len(claimed_messages)} pending messages")
                    # Format as expected by _process_messages
                    formatted = [(self.consumer_config.stream_name, claimed_messages)]
                    await self._process_response(formatted, is_claimed=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error claiming messages: {e}", exc_info=True)

        self.logger.info("Stopped claiming pending messages")

    async def _process_response(self, response: XReadGroupResponseResp2, is_claimed: bool) -> None:
        """Process a batch of messages"""

        is_valid_response_format = validate_xreadgroup_response(response)
        self.logger.info(f"Validate response: {validate_xreadgroup_response(response)}")
        if not is_valid_response_format:
            raise ValueError("Invalid XREADGROUP response format")

        messages = convert_xreadgroup_response(response)
        for message in messages:
            await self._process_single_message(message, is_claimed)

    async def _process_message(self, message: RedisStreamMessage) -> None:
        pass

    async def _process_single_message(
        self,
        message: RedisStreamMessage,
        is_claimed: bool
    ) -> None:
        """Process a single message with timeout and error handling"""
        message_id = message.redis_stream_message_id

        try:
            self.logger.debug(
                f"Processing message: {message}, "
                f"claimed={is_claimed}"
            )

            # Process with timeout
            success = await asyncio.wait_for(
                self.message_handler(message),
                timeout=self.consumer_config.processing_timeout_seconds
            )

            if success:
                # Acknowledge the message (removes from PEL)
                # TODO: should we do multi ACK for batch processing?
                await self._redis_client.xack(
                    self.consumer_config.stream_name,
                    self.consumer_config.consumer_group,
                    message_id,
                )

                # Update metrics
                if is_claimed:
                    self.metrics["messages_claimed"] += 1
                self.metrics["messages_processed"] += 1

                self.logger.debug(f"Successfully processed and acknowledged: {message_id}")
            else:
                # Handler returned False - don't ACK, let it be reclaimed
                self.logger.warning(f"Handler returned False for message: {message_id}")
                self.metrics["messages_failed"] += 1

        except asyncio.TimeoutError:
            self.logger.error(
                f"Message processing timeout: {message_id} "
                f"(>{self.consumer_config.processing_timeout_seconds}s)"
            )
            self.metrics["messages_failed"] += 1
            # Don't ACK - let it be reclaimed

        except Exception as e:
            self.logger.error(
                f"Error processing message {message_id}: {e}",
                exc_info=True
            )
            self.metrics["messages_failed"] += 1
            # Don't ACK - let it be reclaimed

    async def get_metrics(self) -> ConsumerMetrics:
        """Get consumer metrics"""
        metrics = self.metrics.copy()

        if self._redis_client:
            try:
                # Add stream info
                stream_len = await self._redis_client.xlen(self.consumer_config.stream_name)
                metrics["stream_length"] = stream_len

                # Add pending count for this consumer
                pending_info = await self._redis_client.xpending_range(
                    name=self.consumer_config.stream_name,
                    groupname=self.consumer_config.consumer_group,
                    min='-',
                    max='+',
                    count=1,
                    consumername=self.consumer_name
                )
                metrics["pending_count"] = len(pending_info)

            except Exception as e:
                self.logger.error(f"Error getting metrics: {e}")

        return metrics