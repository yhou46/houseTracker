"""
Centralized Logging Factory

A centralized logging system for the houseTracker project.
All modules can use this system to log to a single configurable file with
module-specific prefixes.

Usage:
    from shared.logger_factory import get_logger
    logger = get_logger(__name__)
    logger.info("Processing started")
"""

import logging
import os
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler


class LoggerFactory:
    """Factory class for creating centralized loggers."""

    def __init__(self) -> None:
        self._configured = False
        self._default_log_file_prefix = "house_tracker"
        self._default_log_dir = str(Path(__file__).resolve().parent.parent / "logs")
        self.log_file_path = self._get_log_file_path(self._default_log_dir, self._default_log_file_prefix)
        self.log_level = self._get_log_level()
        self.log_format = '[%(asctime)s] [%(name)s] [%(levelname)s] [%(threadName)s] %(message)s'
        self.date_format = '%Y-%m-%d %H:%M:%S'
        self.enable_console = self._get_console_setting()
        self.max_file_size = 10 * 1024 * 1024  # 10MB
        self.backup_count = 5

    def _get_log_file_path(self, log_dir: str, log_file_prefix: str) -> str:
        """Get log file path from environment or use default."""
        if log_file := os.getenv('HOUSE_TRACKER_LOG_FILE'):
            return log_file

        # Default to logs directory in project root
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(log_dir_path / f"{log_file_prefix}_{timestamp}.log")

    def _get_log_level(self) -> int:
        """Get log level from environment or use default."""
        if log_level := os.getenv('HOUSE_TRACKER_LOG_LEVEL'):
            level_map = {
                'DEBUG': logging.DEBUG,
                'INFO': logging.INFO,
                'WARNING': logging.WARNING,
                'ERROR': logging.ERROR,
                'CRITICAL': logging.CRITICAL
            }
            return level_map.get(log_level.upper(), logging.INFO)
        return logging.INFO

    def _get_console_setting(self) -> bool:
        """Get console logging setting from environment or use default."""
        if console_setting := os.getenv('HOUSE_TRACKER_ENABLE_CONSOLE'):
            return console_setting.lower() in ('true', '1', 'yes', 'on')
        return True

    def _configure_root_logger(self) -> None:
        """Configure the root logger with file and console handlers."""
        # Get root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(self.log_level)

        # Clear existing handlers to avoid duplicates
        root_logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(
            fmt=self.log_format,
            datefmt=self.date_format
        )

        # File handler with rotation
        file_handler = RotatingFileHandler(
            filename=self.log_file_path,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        # Console handler (optional)
        if self.enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.log_level)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

    def configured(self) -> bool:
        """Check if the logger factory has been configured."""
        return self._configured

    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger instance for the specified module.

        Args:
            name: Module name (typically __name__)

        Returns:
            Configured logger instance
        """
        if not self.configured():
            raise RuntimeError("LoggerFactory is not configured yet.")
        return logging.getLogger(name)

    def get_log_file_path(self) -> str:
        """Get the current log file path."""
        return self.log_file_path

    def configure(self,
                   log_dir: str | None = None,
                   log_file_prefix: str | None = None,
                   log_level: int | None = None,
                   enable_console: bool | None = None) -> None:
        """
        Reconfigure the logging system.

        Args:
            log_file_path: New log file path
            log_level: New log level
            enable_console: Whether to enable console logging
        """

        # Update log dir and prefix if provided
        if log_dir or log_file_prefix:
            log_dir = log_dir if log_dir is not None else self._default_log_dir
            log_file_prefix = log_file_prefix if log_file_prefix is not None else self._default_log_file_prefix
            self.log_file_path = self._get_log_file_path(log_dir, log_file_prefix)

        if log_level:
            self.log_level = log_level
        if enable_console is not None:
            self.enable_console = enable_console

        # Reconfigure root logger
        self._configure_root_logger()
        self._configured = True


# Global factory instance
_factory = LoggerFactory()


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified module.

    This is the main function that modules should use to get their logger.

    Args:
        name: Module name (typically __name__)

    Returns:
        Configured logger instance

    Example:
        from shared.logger_factory import get_logger
        logger = get_logger(__name__)
        logger.info("Processing started")
    """
    return _factory.get_logger(name)


def get_log_file_path() -> str:
    """Get the current log file path."""
    return _factory.get_log_file_path()


def configure_logging(
        log_file_path: str | None = None,
        log_file_prefix: str | None = None,
        log_level: int | None = None,
        enable_console: bool | None = None) -> None:
    """
    Reconfigure the logging system.

    Args:
        log_file_path: New log file path
        log_level: New log level
        enable_console: Whether to enable console logging
    """
    _factory.configure(log_file_path, log_file_prefix, log_level, enable_console)
