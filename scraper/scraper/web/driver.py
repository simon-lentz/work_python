import time
from typing import Optional
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import WebDriverException
from pydantic import field_validator, BaseModel, ConfigDict, Field

from scraper.config.logging import StructuredLogger
from scraper.utils.connection import ConnectionData


class DriverConfig(BaseModel):
    """Pydantic model for WebDriver configuration."""
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_to_lower=True,
        str_strip_whitespace=True,
        str_min_length=1,
    )

    host_network: str
    option_args: Optional[list[str]] = None
    proxy: bool = True
    retry_attempts: int = Field(default=3, gt=0)
    retry_interval: int = Field(default=0.5, gt=0)
    user_agent: Optional[str] = None

    @field_validator("host_network")
    @classmethod
    def check_host_network(cls, v: str) -> str:
        if not v:
            raise ValueError("Host network cannot be empty")
        return v


class DriverManager:
    """Manages WebDriver instances for web scraping."""

    def __init__(self, logger: StructuredLogger, cfg: DriverConfig) -> None:
        self.logger = logger
        self.cfg = cfg

    def create_driver(self, connection: ConnectionData) -> Optional[WebDriver]:
        """Creates a WebDriver instance with the specified options and proxy settings."""
        opts = Options()
        for option in self.cfg.option_args or []:
            opts.add_argument(option)
        if self.cfg.proxy:
            opts.add_argument(f"--proxy-server={connection.proxy}")
        if self.cfg.user_agent:
            opts.add_argument(f"--user-agent={self.cfg.user_agent}")
        for attempt in range(self.cfg.retry_attempts):
            try:
                driver = webdriver.Remote(
                    command_executor=f"{self.cfg.host_network}:{connection.port}/wd/hub",
                    options=opts,
                )
                self.logger.info(f"WebDriver session created with session ID: {driver.session_id}")
                return driver
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} - Failed to create driver with proxy '{connection.proxy}': {e}", exc_info=True)  # noqa:E501
                if attempt < self.cfg.retry_attempts - 1:
                    time.sleep(self.cfg.retry_interval)
                else:
                    error_msg = f"Failed to create driver for target '{connection.name}' on port '{connection.port}' after {self.cfg.retry_attempts} attempts"  # noqa:E501
                    self.logger.error(error_msg)
                    raise WebDriverException(error_msg)

    def quit_driver(self, driver: WebDriver) -> None:
        """Quits the WebDriver instance, closing all associated windows."""
        try:
            driver.quit()
            self.logger.info("WebDriver session terminated successfully.")
        except WebDriverException as e:
            self.logger.error(f"Error occurred while terminating WebDriver session: {e}", exc_info=True)
