from typing import Dict, Any, cast
from enum import Enum
import os
import json

from shared.logger_factory import get_logger

class ServiceEnvironment(Enum):
    Local = "local"
    AWS = "aws"

def load_json_config(config_path: str) -> Dict[str, Any]:
    """
    Load and parse JSON configuration file.

    Args:
        config_path: Path to JSON config file

    Returns:
        Parsed configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    if type(config) is not dict:
        raise ValueError(f"Invalid config file format. Config file: {config_path}")

    logger = get_logger(__name__)
    logger.info(f"Load config file from {config_path}")

    return cast(dict[str, Any], config)

def get_config_from_file(
    config_file_prefix: str,
    config_file_path: str,
)-> Dict[str, Any]:

    """
    Get config from config file based on environment variables
    """

    environment_var_name = "SERVICE_ENV"
    config_env = os.getenv(environment_var_name, "")
    env_values = [env.value for env in ServiceEnvironment ]

    if config_env != "" and config_env not in env_values:
        raise ValueError(f"Invalud {environment_var_name} set up, it should be from {env_values}")

    config_suffix = f".{config_env}" if config_env else ""
    config_filename = f"{config_file_prefix}.config{config_suffix}.json"
    config_path = os.path.join(
        config_file_path,
        config_filename
    )
    return load_json_config(config_path)