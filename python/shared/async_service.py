from abc import ABC, abstractmethod
from typing import Callable, Awaitable, Any
import signal
import asyncio

from shared.logger_factory import get_logger


class AsyncService(ABC):
    """Interface for services with async lifecycle"""

    @abstractmethod
    async def start(self) -> None:
        """Start the service and its background tasks"""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the service gracefully"""
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """Check if the service is currently running"""
        pass

# Global flag to ensure run_async_service is only called once per thread
_RUN_ASYNC_SERVICE_CALLED = False

async def run_async_service(
    start_handler: Callable[[], Awaitable[Any]],
    shutdown_handler: Callable[[], Awaitable[None]],
    *,
    shutdown_timeout: float | None = 30.0,
    startup_timeout: float | None = None
) -> int:
    """
    Run an async service until shutdown signal received.
    Service creation, startup and shutdown are fully controlled by the caller.
    It only provides the shutdown signal handling and prevent program from exiting when no shutdown signal is received.
    It should only be called once per thread.

    Args:
        start_handler: Async callable that creates and starts services
        shutdown_handler: Async callable that gracefully stops services
        signals: OS signals that trigger shutdown (default: SIGTERM, SIGINT)
        shutdown_timeout: Max seconds to wait for graceful shutdown (default: 30)
        startup_timeout: Max seconds to wait for service startup (default: None)

    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    global _RUN_ASYNC_SERVICE_CALLED
    if _RUN_ASYNC_SERVICE_CALLED:
        raise RuntimeError("run_async_service can only be called once per thread")
    _RUN_ASYNC_SERVICE_CALLED = True

    logger = get_logger(__name__)
    service = None
    shutdown_event = asyncio.Event()
    shutdown_completed = False  # Track if shutdown was successful

    signals = (signal.SIGTERM, signal.SIGINT)

    def signal_handler() -> None:
        logger.info("Received shutdown signal")
        shutdown_event.set()

    # Register signal handlers
    loop = asyncio.get_running_loop()
    for sig in signals:
        loop.add_signal_handler(sig, signal_handler)

    try:
        # Start the service
        logger.info("Starting service...")
        if startup_timeout:
            service = await asyncio.wait_for(
                start_handler(),
                timeout=startup_timeout
            )
        else:
            service = await start_handler()
        logger.info("Service(s) started successfully")

        # Wait for shutdown signal
        await shutdown_event.wait()

        # Graceful shutdown
        logger.info("Shutting down service...")
        if shutdown_timeout:
            await asyncio.wait_for(
                shutdown_handler(),
                timeout=shutdown_timeout
            )
        else:
            await shutdown_handler()

        shutdown_completed = True
        logger.info("Service stopped successfully")

        return 0

    except asyncio.TimeoutError as e:
        logger.error(f"Timeout error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Error running service: {e}", exc_info=True)
        return 2
    finally:
        # Cleanup signal handlers
        for sig in signals:
            loop.remove_signal_handler(sig)

        # Emergency shutdown if service exists but shutdown failed
        if service is not None and shutdown_event.is_set() and not shutdown_completed:
            logger.info("Performing emergency shutdown...")
            try:
                # Give one last chance for cleanup
                await asyncio.wait_for(
                    shutdown_handler(),
                    timeout=5.0
                )
            except Exception as e:
                logger.error(f"Emergency shutdown failed: {e}")