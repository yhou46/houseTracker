from typing import Any, Callable, Awaitable
from dataclasses import dataclass

import datetime
import asyncio

import redis
import redis.asyncio as redisAsync

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
class RedisStreamConfig:
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

RedisStreamTrimTriggerFunction = Callable[..., Awaitable[bool]]

# TODO: need something to handle shutdown signal across multiple classes
# Ideas: class only provide signal handlers but not register them, rely on caller to register signals
# Need a base class that provides template like start, stop...
class RedisStreamTrimmer(AsyncService):
    """Handles periodic stream trimming with randomization"""

    def __init__(
            self,
            redis_client: redisAsync.Redis,
            config: RedisStreamConfig,
            trim_trigger: RedisStreamTrimTriggerFunction,
            ):
        self.redis = redis_client
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
            length_before = await self.redis.xlen(self.config.stream_name)

            # Execute trim
            if self.config.trim_approximate:
                # Approximate trimming (faster, ~ operator)
                result = await self.redis.xtrim(
                    self.config.stream_name,
                    maxlen=self.config.trim_max_len,
                    approximate=True
                )
            else:
                # Exact trimming
                result = await self.redis.xtrim(
                    self.config.stream_name,
                    maxlen=self.config.trim_max_len,
                    approximate=False
                )

            # Get stream length after trim
            length_after = await self.redis.xlen(self.config.stream_name)

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