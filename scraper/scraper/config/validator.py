import yaml
import json
import toml
import shutil
import psutil
import docker
import requests

from pathlib import Path
from typing import Dict
from requests.exceptions import ConnectionError
from pydantic import ValidationError
from docker.errors import APIError, ImageNotFound

from scraper.utils.exceptions import ConfigError
from scraper.web.proxy import ProxyConfig
from scraper.web.docker import DockerConfig
from scraper.web.driver import DriverConfig
from scraper.etl.target import TargetConfig
from .logging import LoggingConfig


def check_network_connectivity(test_url: str) -> bool:
    try:
        response = requests.get(test_url, timeout=5)
        if response.status_code != 200:
            raise ValueError(f"Network connectivity issue detected. Status code: {response.status_code}")
        return True
    except ConnectionError:
        raise ValueError(f"Network connectivity issue detected. Unable to reach {test_url}")


def check_disk_space(required_space: int | float, path: str = "/") -> bool:
    total, used, free = shutil.disk_usage(path)
    if free < required_space:
        required_space_mb = required_space / (1024 * 1024)
        free_space_mb = free / (1024 * 1024)
        raise ValueError(
            f"Insufficient disk space. Required: {required_space_mb:.2f} MB, "
            f"Free: {free_space_mb:.2f} MB, Path: {path}"
        )
    return True


def check_cpu_usage(threshold: float = 0.9) -> bool:
    current_usage = psutil.cpu_percent() / 100
    if current_usage > threshold:
        raise ValueError(f"CPU usage is too high. Current: {current_usage * 100}%, Threshold: {threshold * 100}%")
    return True


def check_memory_usage(threshold: float = 0.9) -> bool:
    memory = psutil.virtual_memory()
    current_usage = 1 - (memory.available / memory.total)
    if current_usage > threshold:
        raise ValueError(f"Memory usage is too high. Current: {current_usage * 100}%, Threshold: {threshold * 100}%")
    return True


def validate_docker_environment(container_image: str) -> bool:
    client = docker.from_env()
    try:
        if not client.ping():
            raise ValueError("Docker daemon is not running")
        client.images.get(container_image)
    except ImageNotFound:
        raise ValueError(f"Docker image '{container_image}' not found")
    except APIError:
        raise ValueError("Docker daemon is not running")
    return True


def load_config(filename: Path) -> Dict:
    """Loads and validates the configuration from a YAML, JSON, or TOML formatted file."""
    try:
        with open(filename, "r") as file:
            if filename.suffix == ".yaml":
                config_data = yaml.safe_load(file)
            elif filename.suffix == ".json":
                config_data = json.load(file)
            elif filename.suffix == ".toml":
                config_data = toml.load(file)
            else:
                raise ConfigError(f"Unsupported file format: {filename.suffix}")
    except FileNotFoundError as e:
        raise ConfigError(f"Configuration file not found: {filename}") from e
    except (yaml.YAMLError, json.JSONDecodeError, toml.TomlDecodeError) as e:
        raise ConfigError(f"Error parsing configuration: {e}") from e

    try:
        config = {
            "docker": DockerConfig(**config_data.get("Docker", {})),
            "logging": LoggingConfig(**config_data.get("Logging", {})),
            "proxy": ProxyConfig(**config_data.get("Proxy", {})),
            "driver": DriverConfig(**config_data.get("Driver", {})),
            "target": [
                TargetConfig(**target_config)
                for target_config in config_data.get("Target", [])
            ],
        }
        if not validate_docker_environment(config["docker"].container_image):
            raise ValueError("Failed to interface with Docker")
        if not check_network_connectivity("https://www.google.com"):
            raise ValueError("Network connectivity issue detected")
        if not check_disk_space(1 * 1024 * 1024 * 1024):
            raise ValueError("Insufficient disk space")
        if not check_cpu_usage():
            raise ValueError("CPU usage is too high")
        if not check_memory_usage():
            raise ValueError("Memory usage is too high")
        return config
    except ValidationError as e:
        raise ConfigError(f"Failed to validate config: {e}")
