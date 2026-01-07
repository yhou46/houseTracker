
import asyncio
from typing import Any, Awaitable, List

from redis.asyncio import Redis

from shared.async_service import run_async_service
from shared.redis_stream_util import RedisStreamTrimmer, RedisStreamTrimConfig
from shared.logger_factory import configure_logger

from shared.test.redis_stream_test_tools import produce_messages, trim_stream

async def main() -> None:

    configure_logger()

    redis_client_producer = Redis(host='localhost', port=6379, decode_responses=True)
    redis_client_trimmer = Redis(host='localhost', port=6379, decode_responses=True)

    async def should_trim() -> bool:
        return True

    trimmer = RedisStreamTrimmer(
            redis_client_trimmer,
            RedisStreamTrimConfig(
                stream_name='mystream',
                trim_interval_seconds=2,  # Trim every 2 seconds
                trim_max_len=2,
                trim_approximate=False
            ),
            should_trim
        )

    async def start() -> asyncio.Future[Any]:
        msg_producer = produce_messages(
            redis_client_producer,
            message_count=10,
            )

        return asyncio.gather(
            msg_producer,
            trimmer.start()
        )

    async def shutdown() -> None:
        return await trimmer.stop()

    await run_async_service(
        start,
        shutdown,
        )

async def might_fail(input: Any) -> None:
    await asyncio.sleep(0.1)
    print(f"Processing input: {input}")
    raise ValueError("Something went wrong in the task")

async def main2() -> None:
    try:
        await asyncio.wait_for(might_fail(1), timeout=1.0)
        await asyncio.wait_for(might_fail(2), timeout=1.0)

    except ValueError as e:
        print(f"Caught expected exception: {e}")
    except asyncio.TimeoutError:
        print("Operation timed out")

if __name__ == "__main__":
    asyncio.run(main2())