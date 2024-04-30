import time
import json
from typing import Optional
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.proxy import Proxy, ProxyType
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
        opts = Options()
        for option in self.cfg.option_args or []:
            opts.add_argument(option)
        if self.cfg.user_agent:
            opts.add_argument(f"--user-agent={self.cfg.user_agent}")

        if self.cfg.proxy:
            proxy = Proxy()
            proxy.proxy_type = ProxyType.MANUAL
            proxy.http_proxy = connection.proxy
            proxy.ssl_proxy = connection.proxy
            opts.proxy = proxy

        driver = None
        for attempt in range(self.cfg.retry_attempts):
            try:
                driver = webdriver.Remote(
                    command_executor=f"{self.cfg.host_network}:{connection.port}/wd/hub",
                    options=opts
                )
                self.logger.info(f"WebDriver session created with session ID: {driver.session_id}")
                self.logger.info(f"WebDriver session created with capabilities: {driver.capabilities}")

                if self.test_proxy(driver, connection.proxy):
                    return driver
                else:
                    raise Exception("Proxy test failed.")
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} - Failed to create or validate driver: {e}", exc_info=True)
                if driver:
                    driver.quit()
                if attempt >= self.cfg.retry_attempts - 1:
                    error_msg = f"Failed to create driver for '{connection.name}' after {self.cfg.retry_attempts} attempts"  # noqa:E501
                    self.logger.error(error_msg)
                    raise WebDriverException(error_msg)
                time.sleep(self.cfg.retry_interval * (2 ** attempt))

    def test_proxy(self, driver: WebDriver, expected_proxy: str) -> bool:
        test_url = "https://httpbin.org/ip"
        max_attempts = 3
        retry_delay = 1  # Starting delay between retries in seconds

        for attempt in range(max_attempts):
            try:
                driver.get(test_url)
                self.logger.info(f"Retrieved test page: {driver.page_source}")
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "pre"))
                )
                element_text = driver.find_element(By.CSS_SELECTOR, "pre").text
                ip_data = json.loads(element_text)
                actual_ip = ip_data["origin"]
                expected_ip = expected_proxy.split(':')[0]

                if expected_ip == actual_ip:
                    self.logger.info(f"Proxy is functioning correctly on attempt {attempt + 1}")
                    return True
                else:
                    self.logger.error(f"Proxy IP mismatch on attempt {attempt + 1}: expected {expected_ip}, but retrieved {actual_ip}")  # noqa:E501
            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} - Failed to verify proxy: {e}", exc_info=True)
            if attempt < max_attempts - 1:
                time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff

        self.logger.error("All attempts to verify the proxy have failed.")
        return False

    def quit_driver(self, driver: WebDriver) -> None:
        """Quits the WebDriver instance, closing all associated windows."""
        try:
            driver.quit()
            self.logger.info("WebDriver session terminated successfully.")
        except WebDriverException as e:
            self.logger.error(f"Error occurred while terminating WebDriver session: {e}", exc_info=True)


'''
import time
import json
from typing import Optional
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from pydantic import field_validator, BaseModel, ConfigDict, Field

from scraper.config.logging import StructuredLogger


class DriverConfig(BaseModel):
    """Pydantic model for WebDriver configuration."""
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_to_lower=True,
        str_strip_whitespace=True,
        str_min_length=1,
    )

    host_network: Optional[str] = None
    option_args: Optional[list[str]] = None
    input_file: str
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
    """
    Manages WebDriver instances for web scraping.
    Test Config:
    host_network: "http://localhost"
    option_args: ["--headless", "--width=1920", "--height=1080"]
    proxy_file: "./files/proxies/proxy_pool.txt"
    retry_attempts: 3
    retry_interval: 2
    user_agent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36"
    """  # noqa:E501
    def __init__(self, logger: StructuredLogger, cfg: DriverConfig) -> None:
        self.logger = logger
        self.cfg = cfg
        self.functional_proxies = []
        self.raw_proxies = self.load_proxies_from_file()

    def load_proxies_from_file(self) -> list[str]:
        with open(self.cfg.input_file, "r") as file:
            return [line.strip() for line in file if line.strip()]

    def test_all_proxies(self):
        for proxy in self.raw_proxies:
            if self.test_proxy(proxy):
                self.functional_proxies.append(proxy)
                self.logger.info(f"Proxy {proxy} is functional and added to the list.")
            else:
                self.logger.info(f"Proxy {proxy} failed the test and was not added.")

    def create_driver(self) -> Optional[webdriver.Firefox]:
        self.test_all_proxies()
        if not self.functional_proxies:
            self.logger.error("No functional proxies found.")
            return None

        for attempt in range(self.cfg.retry_attempts):
            try:
                driver = webdriver.Firefox(service=FirefoxService(executable_path=GeckoDriverManager().install()), options=self.configure_options(self.functional_proxies[0]))  # noqa:E501
                self.logger.info(f"WebDriver session created with session ID: {driver.session_id}")
                return driver
            except WebDriverException as e:
                self.logger.warning(f"Attempt {attempt + 1} - Failed to create driver: {e}", exc_info=True)
                if attempt >= self.cfg.retry_attempts - 1:
                    self.logger.error("Failed to create driver after maximum retry attempts")
                    raise
                time.sleep(self.cfg.retry_interval * (2 ** attempt))

    def test_proxy(self, proxy: str) -> bool:
        test_url = "https://httpbin.org/ip"
        try:
            opts = Options()
            opts.add_argument(f"--proxy-server={proxy}")
            driver = webdriver.Firefox(options=opts, service=FirefoxService(executable_path=GeckoDriverManager().install()))  # noqa:E501
            driver.get(test_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "pre")))
            element_text = driver.find_element(By.CSS_SELECTOR, "pre").text
            ip_data = json.loads(element_text)
            actual_ip = ip_data["origin"]
            driver.quit()
            return actual_ip.split(':')[0] == proxy.split(':')[0]
        except Exception as e:
            self.logger.info(f"Proxy {proxy} failed with error: {e}")
            if driver:
                driver.quit()
            return False

    def quit_driver(self, driver: webdriver.Firefox) -> None:
        try:
            driver.quit()
            self.logger.info("WebDriver session terminated successfully.")
        except WebDriverException as e:
            self.logger.error(f"Error occurred while terminating WebDriver session: {e}", exc_info=True)
'''  # noqa:E501
