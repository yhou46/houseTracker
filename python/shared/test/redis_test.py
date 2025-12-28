
from typing import Dict, Union, cast
import asyncio
import json
from redis.asyncio import Redis
from datetime import datetime

import shared.redis_util as redis_util
from shared.logger_factory import configure_logger


RedisFieldType = Union[bytes, bytearray, memoryview, str, int, float]
RedisFields = dict[RedisFieldType, RedisFieldType]

async def produce_messages(redis_client: Redis) -> None:
    """Async Redis stream producer"""

    try:


        # Produce messages
        for i in range(10):
            message: dict[Union[str, bytes], Union[str, int, float, bytes]] = {
                'id': f"{i}",
                'timestamp': datetime.now().isoformat(),
                'data': f'Message {i}',
                'value': f"{100 + i}"
            }

            # Add message to stream
            message_id = await redis_client.xadd(
                'mystream',  # stream name
                #
                fields=cast(RedisFields, message)      # message data
            )

            print(f"Produced: {message_id} - {message}")
            await asyncio.sleep(0.5)  # Delay between messages

    finally:
        await redis_client.aclose()
        print("Disconnected from Redis")

async def trim_stream(
    redis: Redis,
    stream_name: str,
    max_len: int,
    approximate: bool = True
) -> None:
    """Trim the Redis stream to maintain a maximum length."""

    async def should_trim() -> bool:

        return True

    trimmer = redis_util.RedisStreamTrimmer(
        redis,
        redis_util.RedisStreamConfig(
            stream_name=stream_name,
            trim_interval_seconds=2,  # Trim every 2 seconds
            trim_max_len=max_len,
            trim_approximate=approximate
        ),
        should_trim
    )

    await trimmer.start()

# Run the producer
def main() -> None:
    # redis_config = redis_util.RedisConfig(
    #     "localhost",
    #     6379,
    #     None,
    # )

    # redis_client = redis_util.RedisClient(redis_config)

    # redis_client.setKey("test_key", "test_value")
    # value = redis_client.getKey("test_key")
    # print(f"Value for 'test_key': {value}")

    redis_client = Redis(host='localhost', port=6379, decode_responses=True)
    # asyncio.run(produce_messages(redis_client))

    configure_logger()
    asyncio.run(trim_stream(
        redis_client,
        'mystream',
        max_len=2,
        approximate=False,
    ))



if __name__ == "__main__":
    main()