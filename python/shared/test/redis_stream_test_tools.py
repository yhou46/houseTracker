
from typing import Dict, Union, cast, Any
import asyncio
import json
import argparse
from datetime import datetime


from redis.asyncio import Redis

import shared.redis_util as redis_util
from shared.logger_factory import configure_logger
from shared.async_service import run_async_service


# RedisFieldType = Union[bytes, bytearray, memoryview, str, int, float]
# RedisFields = dict[RedisFieldType, RedisFieldType]

async def produce_messages(redis_client: Redis, message_count: int) -> None:
    """Async Redis stream producer"""

    try:


        # Produce messages
        for i in range(message_count):
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
                fields=cast(redis_util.RedisFields, message)      # message data
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
        redis_util.RedisStreamTrimConfig(
            stream_name=stream_name,
            trim_interval_seconds=1 * 60,  # Trim every 1 minute
            trim_max_len=max_len,
            trim_approximate=approximate
        ),
        should_trim
    )

    await trimmer.start()

async def consume_stream(
    redis_client: Redis,
    stream_name: str,
    max_stream_size: int,
    approximate: bool = True,
) -> None:
    """Async Redis stream consumer"""

    trim_config = redis_util.RedisStreamTrimConfig(
            stream_name=stream_name,
            trim_interval_seconds=30,  # Trim every 2 seconds
            trim_max_len=max_stream_size,
            trim_approximate=approximate
    )

    consumer_config = redis_util.RedisStreamConsumerConfig(
        stream_name=stream_name,
        consumer_group="my_consumer_group",
        consumer_name_prefix="consumer",
    )

    async def message_handler(message: redis_util.RedisStreamMessage) -> bool:
        print(f"Consumed message: {message}")
        return True  # Acknowledge message

    consumer = redis_util.RedisStreamConsumer(
        consumer_config,
        trim_config,
        redis_util.always_trim,
        redis_client,
        message_handler,
    )

    # await consumer.start()

    await run_async_service(
        consumer.start,
        consumer.stop,
    )

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

    parser = argparse.ArgumentParser(
        description="Test tools for Redis stream producer and consumer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python redis_stream_test_tools.py --mode "producer" --count 100
  python redis_stream_test_tools.py --mode "consumer"
        """
    )

    # Query options (mutually exclusive)
    parser.add_argument(
        '--mode',
        type=str,
        required=True,
        choices=['producer', 'consumer', 'trimmer'],
        help='Operation mode (producer or consumer)'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=100,
        help='Number of messages to produce (for producer mode)'
    )
    parser.add_argument(
        '--maxlen',
        type=int,
        default=1000,
        help='Maximum length of the stream (for consumer and trimmer modes)'
    )
    args = parser.parse_args()

    redis_client = Redis(host='localhost', port=6379, decode_responses=True)

    configure_logger()

    if args.mode == 'producer':
        # Produce test
        asyncio.run(produce_messages(redis_client, args.count))
    elif args.mode == 'consumer':
        # Consume test
        asyncio.run(consume_stream(
            redis_client,
            'mystream',
            max_stream_size=500,
            approximate=False,
        ))
    elif args.mode == 'trimmer':
        # Trim test
        asyncio.run(trim_stream(
            redis_client,
            'mystream',
            max_len=args.maxlen,
            approximate=False,
        ))
    else:
        print(f"Unknown mode: {args.mode}")
        return

    # Trim test
    # asyncio.run(trim_stream(
    #     redis_client,
    #     'mystream',
    #     max_len=2,
    #     approximate=False,
    # ))

    # Consume test
    # asyncio.run(consume_stream(
    #     redis_client,
    #     'mystream',
    #     max_len=2,
    #     approximate=False,
    # ))


if __name__ == "__main__":
    main()