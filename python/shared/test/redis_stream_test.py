import unittest
import asyncio
import json
from pathlib import Path
from typing import Any, List, Dict, cast

from redis.asyncio import Redis

from shared.redis_stream_util import (
    RedisStreamTrimmer,
    RedisStreamTrimConfig,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    RedisStreamMessage,
    RedisStreamMessageHandler,
    always_trim,
    RedisFields,
)
from shared.logger_factory import configure_logger, get_logger


# Module-level helper functions

async def populate_stream(
    redis_client: Redis,
    stream_name: str,
    message_count: int
) -> List[str]:
    """Populate stream with test messages and return message IDs"""
    message_ids = []
    for i in range(message_count):
        message = {
            'id': str(i),
            'data': f'Test message {i}'
        }
        msg_id = await redis_client.xadd(stream_name, fields=cast(RedisFields, message))
        if not isinstance(msg_id, str):
            raise TypeError(f"Expected str from xadd, got {type(msg_id)}")
        message_ids.append(msg_id)
    return message_ids


async def get_stream_length(redis_client: Redis, stream_name: str) -> int:
    """Get current stream length"""
    length = await redis_client.xlen(stream_name)
    if not isinstance(length, int):
        raise TypeError(f"Expected int from xlen, got {type(length)}")
    return length


class TestRedisStreamTrimmer(unittest.IsolatedAsyncioTestCase):
    """Test suite for RedisStreamTrimmer"""

    """
    Public test methods inherit from unittest.IsolatedAsyncioTestCase
    """
    async def asyncSetUp(self) -> None:
        """Set up test fixtures before each test"""
        configure_logger()
        self.logger = get_logger(self.__class__.__name__)

        # Load Redis config using relative path
        current_dir = Path(__file__).parent
        config_path = current_dir / "config" / "redis_test.config.json"
        with open(config_path, 'r') as f:
            redis_config = json.load(f)

        # Create async Redis client
        self.redis_client = Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            decode_responses=True
        )

        # Test stream name (unique per test to avoid conflicts)
        self.stream_name_prefix = "test_redis_stream"
        self.stream_name = f"{self.stream_name_prefix}_{id(self)}"

    async def asyncTearDown(self) -> None:
        """Clean up after each test"""
        # Delete test stream
        try:
            await self.redis_client.delete(self.stream_name)
        except Exception as e:
            self.logger.error(f"Failed to delete test stream {self.stream_name}: {e}")

        # Log remaining streams to verify cleanup
        self.assertEqual(
            await self._log_all_test_streams(),
            [],
            "There should be no remaining test streams after cleanup"
        )

        # Close Redis connection
        await self.redis_client.aclose()

    # Reusable helper methods

    async def _create_trimmer(
        self,
        trim_interval_seconds: int = 1,
        trim_max_len: int = 10,
        trim_approximate: bool = False
    ) -> RedisStreamTrimmer:
        """Create a RedisStreamTrimmer instance with test configuration"""

        config = RedisStreamTrimConfig(
            stream_name=self.stream_name,
            trim_interval_seconds=trim_interval_seconds,
            trim_max_len=trim_max_len,
            trim_approximate=trim_approximate
        )

        return RedisStreamTrimmer(
            self.redis_client,
            config,
            always_trim
        )

    async def _populate_stream(self, message_count: int) -> List[str]:
        """Populate the test stream with messages"""
        return await populate_stream(self.redis_client, self.stream_name, message_count)

    async def _get_stream_length(self) -> int:
        """Get current stream length"""
        return await get_stream_length(self.redis_client, self.stream_name)

    async def _log_all_test_streams(self) -> List[Any]:
        """Log all test streams currently in Redis"""
        try:
            all_keys = await self.redis_client.keys(f"{self.stream_name_prefix}_*")
            if all_keys:
                self.logger.info(f"Current test streams in Redis: {all_keys}")
            else:
                self.logger.info("No test streams in Redis")

            if not isinstance(all_keys, list):
                raise TypeError(f"Expected list from keys, got {type(all_keys)}")

            return all_keys
        except Exception as error:
            self.logger.error(f"Failed to list streams: {error}")
            raise error

    # Test scenarios

    async def test_start_stop_and_basic_trim(self) -> None:
        """Test that trimmer can start, trim messages, and stop properly"""

        # Populate stream with more messages than max length BEFORE starting trimmer
        message_count = 25
        await self._populate_stream(message_count)

        # Verify stream has all messages before trim
        initial_length = await self._get_stream_length()
        self.assertEqual(initial_length, message_count,
                        f"Stream should have {message_count} messages initially")

        # Create trimmer with short interval for faster testing
        trim_max_len = 10
        trimmer = await self._create_trimmer(
            trim_interval_seconds=1,
            trim_max_len=trim_max_len,
            trim_approximate=False
        )

        # Start the trimmer
        await trimmer.start()
        self.assertTrue(trimmer.is_running(), "Trimmer should be running after start()")

        # Wait for trim cycle to execute (interval + processing time)
        await asyncio.sleep(2)

        # Verify messages are trimmed to max length
        trimmed_length = await self._get_stream_length()
        self.assertEqual(trimmed_length, trim_max_len,
                        f"Stream should be trimmed to {trim_max_len} messages")

        # Stop the trimmer
        await trimmer.stop()
        self.assertFalse(trimmer.is_running(), "Trimmer should not be running after stop()")

    async def test_force_trim(self) -> None:
        """Test that force_trim triggers immediate trimming without waiting for interval"""

        # Populate stream with more messages than max length
        message_count = 20
        await self._populate_stream(message_count)

        # Verify stream has all messages
        initial_length = await self._get_stream_length()
        self.assertEqual(initial_length, message_count,
                        f"Stream should have {message_count} messages initially")

        # Create trimmer with long interval to ensure force_trim is what triggers trim
        trim_max_len = 5
        trimmer = await self._create_trimmer(
            trim_interval_seconds=60,  # Long interval - should not trigger during test
            trim_max_len=trim_max_len,
            trim_approximate=False
        )

        # Start the trimmer
        await trimmer.start()
        self.assertTrue(trimmer.is_running(), "Trimmer should be running after start()")

        # Call force_trim to trigger immediate trim
        await trimmer.force_trim()

        # Verify messages are trimmed immediately without waiting for interval
        trimmed_length = await self._get_stream_length()
        self.assertEqual(trimmed_length, trim_max_len,
                        f"Stream should be trimmed to {trim_max_len} messages after force_trim()")

        # Stop the trimmer
        await trimmer.stop()
        self.assertFalse(trimmer.is_running(), "Trimmer should not be running after stop()")

    async def test_trimmer_handles_missing_stream_and_errors(self) -> None:
        """Test that trimmer starts and stops properly when stream doesn't exist or has errors"""

        # Create trimmer for a stream that doesn't exist yet
        trim_max_len = 10
        trimmer = await self._create_trimmer(
            trim_interval_seconds=1,
            trim_max_len=trim_max_len,
            trim_approximate=False
        )

        # Start the trimmer - should succeed even though stream doesn't exist
        await trimmer.start()
        self.assertTrue(trimmer.is_running(), "Trimmer should be running even with no stream")

        # Wait for trim cycle to execute on non-existent stream
        # This should not crash - trimmer should handle gracefully
        await asyncio.sleep(2)

        # Verify trimmer is still running after attempting to trim non-existent stream
        self.assertTrue(trimmer.is_running(), "Trimmer should still be running after trim attempts")

        # Now create the stream with some messages
        message_count = 15
        await self._populate_stream(message_count)

        # Verify stream was created, since trimmer could happen just verify message exist in the stream
        initial_length = await self._get_stream_length()
        self.assertGreater(initial_length, 0,
                        f"Stream should have more than 0 messages after population")

        # Wait for another trim cycle to process the newly created stream
        await asyncio.sleep(2)

        # Verify trimmer successfully trimmed the stream after it was created
        trimmed_length = await self._get_stream_length()
        self.assertEqual(trimmed_length, trim_max_len,
                        f"Stream should be trimmed to {trim_max_len} messages")

        # Stop the trimmer successfully
        await trimmer.stop()
        self.assertFalse(trimmer.is_running(), "Trimmer should not be running after stop()")

    async def test_no_trim_when_below_max_length(self) -> None:
        """Test that trimmer does not remove messages when stream length is below max"""

        # Populate stream with LESS messages than max length
        trim_max_len = 10
        message_count = 5  # Less than trim_max_len
        await self._populate_stream(message_count)

        # Verify stream has all messages
        initial_length = await self._get_stream_length()
        self.assertEqual(initial_length, message_count,
                        f"Stream should have {message_count} messages initially")

        # Create trimmer
        trimmer = await self._create_trimmer(
            trim_interval_seconds=1,
            trim_max_len=trim_max_len,
            trim_approximate=False
        )

        # Start the trimmer
        await trimmer.start()
        self.assertTrue(trimmer.is_running(), "Trimmer should be running after start()")

        # Wait for trim cycle to execute
        await asyncio.sleep(2)

        # Verify NO messages were removed (stream length unchanged)
        final_length = await self._get_stream_length()
        self.assertEqual(final_length, message_count,
                        f"Stream should still have {message_count} messages (no trimming should occur)")

        # Stop the trimmer
        await trimmer.stop()
        self.assertFalse(trimmer.is_running(), "Trimmer should not be running after stop()")



class TestRedisStreamConsumer(unittest.IsolatedAsyncioTestCase):
    """Test suite for RedisStreamConsumer"""

    async def asyncSetUp(self) -> None:
        """Set up test fixtures before each test"""
        configure_logger()
        self.logger = get_logger(self.__class__.__name__)

        # Load Redis config using relative path
        current_dir = Path(__file__).parent
        config_path = current_dir / "config" / "redis_test.config.json"
        with open(config_path, 'r') as f:
            redis_config = json.load(f)

        # Create async Redis client
        self.redis_client = Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            decode_responses=True
        )

        # Test stream name (unique per test to avoid conflicts)
        self.stream_name_prefix = "test_redis_stream_consumer"
        self.stream_name = f"{self.stream_name_prefix}_{id(self)}"
        self.consumer_group = "test_group"

        # Shared storage for tracking processed messages (simulates real app state)
        # Key: message_id, Value: dict with consumer_name, timestamp, data, etc.
        self.processed_messages: Dict[str, Dict[str, Any]] = {}

    async def asyncTearDown(self) -> None:
        """Clean up after each test"""
        # Delete consumer group first
        try:
            await self.redis_client.xgroup_destroy(self.stream_name, self.consumer_group)
            self.logger.info(f"Deleted consumer group: {self.consumer_group}")
        except Exception as e:
            self.logger.info(f"Consumer group cleanup (may not exist): {e}")

        # Delete test stream
        try:
            await self.redis_client.delete(self.stream_name)
        except Exception as e:
            self.logger.error(f"Failed to delete test stream {self.stream_name}: {e}")

        # Log remaining streams to verify cleanup
        remaining_streams = await self._log_all_test_streams()
        self.assertEqual(
            remaining_streams,
            [],
            "There should be no remaining test streams after cleanup"
        )

        # Close Redis connection
        await self.redis_client.aclose()

    # Reusable helper methods

    async def _create_consumer(
        self,
        consumer_name_prefix: str = "test_consumer",
        consumer_group: str | None = None,
        block_ms: int = 1000,
        count: int = 10,
        claim_interval_seconds: int = 2,
        claim_idle_ms: int = 3000,
        processing_timeout_seconds: int = 45,
        message_handler: RedisStreamMessageHandler | None = None,
        trim_interval_seconds: int = 60,
        trim_max_len: int = 1000,
    ) -> RedisStreamConsumer:
        """Create a RedisStreamConsumer with test configuration"""
        if consumer_group is None:
            consumer_group = self.consumer_group

        consumer_config = RedisStreamConsumerConfig(
            stream_name=self.stream_name,
            consumer_group=consumer_group,
            consumer_name_prefix=consumer_name_prefix,
            read_block_ms=block_ms,
            read_batch_size=count,
            claim_interval_seconds=claim_interval_seconds,
            claim_idle_ms=claim_idle_ms,
            processing_timeout_seconds=processing_timeout_seconds,
            shutdown_grace_period_seconds=10,
        )

        trimmer_config = RedisStreamTrimConfig(
            stream_name=self.stream_name,
            trim_interval_seconds=trim_interval_seconds,
            trim_max_len=trim_max_len,
            trim_approximate=True,
        )

        # Use provided handler or create default tracking handler
        if message_handler is None:
            message_handler = self._create_tracking_handler(consumer_name_prefix)

        return RedisStreamConsumer(
            consumer_config=consumer_config,
            trimmer_config=trimmer_config,
            trim_trigger=always_trim,
            redis_client=self.redis_client,
            message_handler=message_handler,
            debug=True,
        )

    def _create_tracking_handler(
        self,
        consumer_name: str,
        should_fail: bool = False,
        fail_message_ids: set[str] | None = None,
        processing_delay_seconds: float = 0.01,
    ) -> RedisStreamMessageHandler:
        """Create a handler that tracks processed messages in shared storage"""

        async def handler(message: RedisStreamMessage) -> None:
            # Check if should fail for this message
            if should_fail or (fail_message_ids and message.redis_stream_message_id in fail_message_ids):
                self.logger.info(f"Handler failing for message: {message.redis_stream_message_id}")
                raise ValueError(f"Simulated failure for message {message.redis_stream_message_id}")

            # Track in shared storage
            self.processed_messages[message.redis_stream_message_id] = {
                'consumer_name': consumer_name,
                'data': message.data,
                'timestamp': asyncio.get_event_loop().time(),
                'stream_name': message.stream_name,
            }

            self.logger.info(f"Handler processed message: {message.redis_stream_message_id} by {consumer_name}")

            # Simulate some processing time
            await asyncio.sleep(processing_delay_seconds)

        return handler

    async def _wait_for_processing(
        self,
        expected_count: int,
        timeout_seconds: int = 15,
        poll_interval: float = 0.5,
    ) -> bool:
        """Poll until expected number of messages are processed in shared storage"""
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < timeout_seconds:
            current_count = len(self.processed_messages)

            self.logger.info(f"Waiting for processing: {current_count}/{expected_count}")

            if current_count >= expected_count:
                return True

            await asyncio.sleep(poll_interval)

        return False

    async def _get_pending_count(self, consumer_group: str | None = None) -> int:
        """Get total pending message count for a consumer group"""
        if consumer_group is None:
            consumer_group = self.consumer_group

        try:
            # Get pending info for the consumer group
            pending_info = await self.redis_client.xpending(
                name=self.stream_name,
                groupname=consumer_group,
            )
            # xpending returns dict with 'pending' count
            if isinstance(pending_info, dict) and 'pending' in pending_info:
                pending_count = pending_info['pending']
            elif isinstance(pending_info, list) and len(pending_info) > 0:
                # Format: [pending_count, min_id, max_id, consumers]
                pending_count = pending_info[0]
            else:
                pending_count = 0

            if not isinstance(pending_count, int):
                raise TypeError(f"Expected int for pending count, got {type(pending_count)}")

            return pending_count
        except Exception as e:
            self.logger.error(f"Error getting pending count: {e}")
            return 0

    async def _log_all_test_streams(self) -> List[Any]:
        """Log all test streams currently in Redis"""
        try:
            all_keys = await self.redis_client.keys(f"{self.stream_name_prefix}_*")
            if all_keys:
                self.logger.info(f"Current test streams in Redis: {all_keys}")
            else:
                self.logger.info("No test streams in Redis")

            if not isinstance(all_keys, list):
                raise TypeError(f"Expected list from keys, got {type(all_keys)}")

            return all_keys
        except Exception as error:
            self.logger.error(f"Failed to list streams: {error}")
            raise error

    # Test scenarios

    async def test_single_consumer_batch_processing(self) -> None:
        """Test single consumer starts, processes batch of messages, and stops cleanly"""

        # Setup: Populate stream with messages BEFORE starting consumer
        message_count = 25  # More than count config (10)
        await populate_stream(self.redis_client, self.stream_name, message_count)

        self.logger.info(f"Populated stream with {message_count} messages")

        # Create consumer with batch size of 10
        consumer = await self._create_consumer(
            consumer_name_prefix="batch_consumer",
            count=10,
            block_ms=1000,
        )

        # Start consumer
        await consumer.start()
        self.assertTrue(consumer.is_running(), "Consumer should be running after start()")

        # Wait for all messages to be processed (polls with timeout)
        success = await self._wait_for_processing(
            expected_count=message_count,
            timeout_seconds=20,
        )
        self.assertTrue(success, f"All {message_count} messages should be processed")

        # Verify all messages were processed
        processed_count = len(self.processed_messages)
        self.assertEqual(processed_count, message_count, "All messages should be tracked in storage")

        # Verify metrics match our ground truth
        metrics = await consumer.get_metrics()
        self.assertEqual(
            metrics["messages_processed"],
            message_count,
            "Metrics should match actual processed count"
        )
        self.assertEqual(metrics["messages_failed"], 0, "No messages should fail")
        self.assertEqual(metrics["messages_claimed"], 0, "No messages should be claimed (all fresh reads)")

        # Verify stream state (messages should still be in stream until trimmed)
        stream_length = await get_stream_length(self.redis_client, self.stream_name)
        self.assertEqual(stream_length, message_count, "Messages remain in stream")

        # Verify no pending messages (all ACKed)
        pending_count = await self._get_pending_count()
        self.assertEqual(pending_count, 0, "No pending messages - all should be ACKed")

        # Stop consumer cleanly
        await consumer.stop()
        self.assertFalse(consumer.is_running(), "Consumer should not be running after stop()")

        # Verify consumer group was created and exists
        # Check by trying to get group info
        groups = await self.redis_client.xinfo_groups(self.stream_name)
        self.assertIsInstance(groups, list)
        self.assertGreater(len(groups), 0, "Consumer group should exist")

        group_names = [g['name'] for g in groups]
        self.assertIn(self.consumer_group, group_names, "Our consumer group should exist")

        self.logger.info("Test completed successfully")


if __name__ == "__main__":
    unittest.main()
