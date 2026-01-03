import unittest
import asyncio
import json
from pathlib import Path
from typing import Any, List, cast

from redis.asyncio import Redis

from shared.redis_stream_util import RedisStreamTrimmer, RedisStreamTrimConfig, always_trim, RedisFields
from shared.logger_factory import configure_logger, get_logger


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

    async def _populate_stream(self, message_count: int) -> None:
        """Populate the test stream with messages"""
        for i in range(message_count):
            message = {
                'id': str(i),
                'data': f'Test message {i}'
            }
            await self.redis_client.xadd(self.stream_name, fields=cast(RedisFields, message))

    async def _get_stream_length(self) -> int:
        """Get current stream length"""
        length = await self.redis_client.xlen(self.stream_name)
        if not isinstance(length, int):
            raise TypeError(f"Expected int from xlen, got {type(length)}")
        return length

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


if __name__ == "__main__":
    unittest.main()
