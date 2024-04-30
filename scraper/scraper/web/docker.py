import time
import docker
from typing import Optional
from docker.models.containers import Container
from docker.errors import ContainerError, APIError, NotFound
from pydantic import field_validator, BaseModel, ConfigDict, Field

from scraper.config.logging import StructuredLogger
from scraper.utils.connection import ConnectionData


class DockerConfig(BaseModel):
    """Pydantic model for Docker configuration."""
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_to_lower=True,
        str_strip_whitespace=True,
        str_min_length=1,
    )

    ports: list[int]
    container_shm_size: str = Field(..., pattern=r"^\d+[KMGBkmgb][Bb]?$")
    container_image: str
    remove_on_cleanup: bool = True
    environment: Optional[dict[str, str]] = None
    network_mode: str = "bridge"
    resource_limits: Optional[dict[str, str]] = None

    @field_validator("ports")
    def check_ports(cls, v):
        if len(v) == 0:
            raise ValueError("Must specify at least one port value")
        for port in v:
            if not (0 <= port <= 65535):
                raise ValueError("Port value out of valid range")
        return v

    @field_validator("environment")
    def check_environment(cls, v: dict[str, str]) -> dict[str, str]:
        if not isinstance(v, dict):
            raise ValueError("Environment must be a dictionary")
        for key, value in v.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError("Environment keys and values must be strings")
        return v

    @field_validator("network_mode")
    def check_network(cls, v: str) -> str:
        allowed = ["bridge", "host", "none"]
        if v not in allowed:
            raise ValueError(
                f"Network mode '{v}' is not supported. Allowed modes are: {', '.join(allowed)}"
            )
        return v


class DockerManager:
    """Manages Docker containers for web scraping."""

    def __init__(self, logger: StructuredLogger, cfg: DockerConfig) -> None:
        self.logger = logger
        self.cfg = cfg
        self.client = docker.from_env()

    def create_container(self, connection: ConnectionData) -> Optional[Container]:
        """Creates and starts a Docker container."""
        try:
            container = self.client.containers.run(
                self.cfg.container_image,
                name=connection.name,
                ports={"4444/tcp": connection.port},
                detach=True,
                network_mode=self.cfg.network_mode,
                shm_size=self.cfg.container_shm_size,
                environment=self.cfg.environment,
            )
            self.logger.info(f"'{connection.name}' browser started on port '{connection.port}'")
            return container
        except (ContainerError, APIError) as e:
            self.logger.error(f"'{connection.name}' browser failed to start: {e}")
            raise

    def cleanup(self, container: Container) -> None:
        """Attempts to gracefully stop and remove a Docker container with retry logic."""
        try:
            self.stop_container(container)
        except Exception as e:
            self.logger.error(f"Initial attempt to stop container '{container.name}' failed: {e}", exc_info=True)
        except KeyboardInterrupt:
            self.kill_container(container)
        finally:
            if self.cfg.remove_on_cleanup:
                self.remove_container(container)

    def stop_container(self, container: Container) -> None:
        """Stops a Docker container with a timeout and retry logic."""
        retry_limit = 3
        for attempt in range(retry_limit):
            try:
                container.stop(timeout=10)
                self.logger.info(f"Container '{container.name}' stopped successfully.")
                return
            except (ContainerError, APIError) as e:
                self.logger.error(f"Attempt {attempt + 1} to stop container '{container.name}' failed: {e}")
                if attempt < retry_limit - 1:
                    time.sleep(2)  # wait before retrying
                else:
                    self.logger.error(f"Final attempt to stop container '{container.name}' also failed. Killing...")
                    self.kill_container(container)  # Force kill if stop fails repeatedly
            except KeyboardInterrupt as e:
                raise e

    def kill_container(self, container: Container):
        """Forcefully kills a Docker container."""
        try:
            container.kill()
            self.logger.info(f"Container '{container.name}' was forcefully killed.")
        except (ContainerError, APIError) as e:
            self.logger.error(f"Failed to kill container '{container.name}': {e}")
            raise

    def remove_container(self, container: Container) -> None:
        """Removes a Docker container."""
        try:
            container.remove()
            self.logger.info(f"Removed container '{container.name}' (ID: {container.id}).")
        except NotFound:
            self.logger.warning(f"Container '{container.name}' does not exist or has already been removed.")
        except (ContainerError, APIError) as e:
            self.logger.error(f"Failed to remove container '{container.name}': {e}")
