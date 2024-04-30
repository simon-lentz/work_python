import json
from pathlib import Path
from typing import Optional
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.proxy import Proxy, ProxyType
# from selenium.webdriver.firefox.service import Service as FirefoxService
# from webdriver_manager.firefox import GeckoDriverManager
from pydantic import field_validator, BaseModel, ConfigDict, Field

from scraper.config.logging import StructuredLogger
from scraper.utils.exceptions import UsageError, ProxyReloadError


class ProxyConfig(BaseModel):
    """Pydantic model for proxy configuration."""
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_to_lower=True,
        str_strip_whitespace=True,
        str_min_length=1,
    )

    input_file: Path
    test_url: str = Field(..., pattern=r"^https?://")
    usage_limit: int = Field(..., gt=0)
    validation: bool = True
    proxy_type: str = Field(..., pattern=r"^(HTTP|HTTPS|SOCKS4|SOCKS5)$")
    authentication: Optional[dict[str, str]] = None

    @field_validator("input_file")
    @classmethod
    def check_file_exists(cls, v: Path) -> Path:
        if v.exists() and v.is_file():
            return v
        else:
            raise ValueError(f"Proxy pool input file '{v}' not found")

    @field_validator("authentication")
    @classmethod
    def check_authentication(cls, v: Optional[dict[str, str]]) -> Optional[dict[str, str]]:
        if v is not None:
            if "username" not in v or "password" not in v:
                raise ValueError("Authentication must include both 'username' and 'password' keys")
        return v


class ProxyManager:
    """Manages a pool of proxies for web scraping."""

    def __init__(self, logger: StructuredLogger, cfg: ProxyConfig) -> None:
        self.logger = logger
        self.cfg = cfg
        self.proxy_pool: dict[str, tuple[int, bool]] = {}
        self.initialize_proxy_pool()
        self.logger.info(f"Initialized proxy pool, {len(self.proxy_pool)} proxies available.")

    def validate_proxies(self, raw_proxies: list[str]) -> list[str]:
        test_url = "https://httpbin.org/ip"
        validated_proxies = []
        for proxy in raw_proxies:
            driver = None
            try:
                opts = Options()
                selenium_proxy = Proxy()
                selenium_proxy.proxy_type = ProxyType.MANUAL
                selenium_proxy.http_proxy = proxy
                #  selenium_proxy.ssl_proxy = proxy
                opts.proxy = selenium_proxy
                #  opts.add_argument("--headless")
                driver = webdriver.Firefox(options=opts)
                driver.get(test_url)

                # Log the driver capabilities to check if proxies are set correctly
                self.logger.info(f"WebDriver capabilities: {driver.capabilities}")

                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "json")))
                element_text = driver.find_element(By.ID, "json").text
                ip_data = json.loads(element_text)
                actual_ip = ip_data["origin"]
                if actual_ip.split(':')[0] == proxy.split(':')[0]:
                    validated_proxies.append(proxy)
                else:
                    self.logger.info(f"Proxy {proxy} failed validation. Expected IP part {proxy.split(':')[0]}, but got {actual_ip.split(':')[0]}")  # noqa:E501
            except Exception as e:
                self.logger.info(f"Proxy {proxy} failed with error: {e}")
            finally:
                if driver:
                    driver.quit()
        return validated_proxies

    def quit_driver(self, driver: webdriver.Firefox) -> None:
        try:
            driver.quit()
            self.logger.info("WebDriver session terminated successfully.")
        except WebDriverException as e:
            self.logger.error(f"Error occurred while terminating WebDriver session: {e}", exc_info=True)

    def initialize_proxy_pool(self) -> None:
        """Initializes the proxy pool based on the configuration."""
        proxies = self.load_proxies_from_file()
        if self.cfg.validation:
            proxies = self.validate_proxies(proxies)
        self.proxy_pool = {proxy: (0, False) for proxy in proxies}

    def load_proxies_from_file(self) -> list[str]:
        """Loads the list of proxies from the input file."""
        with open(self.cfg.input_file, "r") as file:
            return [line.strip() for line in file if line.strip()]

    def format_proxy_url(self, proxy: str) -> str:
        """Formats the proxy URL based on the proxy type and authentication."""
        protocol = self.cfg.proxy_type.lower()
        auth = f"{self.cfg.authentication['username']}:{self.cfg.authentication['password']}@" if self.cfg.authentication else ""  # noqa:E501
        return f"{protocol}://{auth}{proxy}"

    def remove_proxy_from_pool(self, proxy: str) -> None:
        """Removes a proxy from the pool."""
        if proxy in self.proxy_pool:
            del self.proxy_pool[proxy]
            self.logger.info(f"Proxy '{proxy}' removed from the pool")

    def get_available_proxy(self) -> str:
        """Retrieves an available proxy from the pool or reloads the pool if necessary."""
        for proxy, (usage, in_use) in self.proxy_pool.items():
            if usage < self.cfg.usage_limit and not in_use:
                self.proxy_pool[proxy] = (usage + 1, True)
                return proxy
        self.logger.info("Proxy pool exhausted, reloading proxy pool...")
        return self.reload_proxy_pool()

    def reload_proxy_pool(self) -> str:
        """Reloads the proxy pool with fresh proxies and retrieves an available proxy."""
        new_proxies = set(self.load_proxies_from_file()) - set(self.proxy_pool.keys())
        if not new_proxies:
            self.logger.error("No proxies available to refresh exhausted proxy pool.")
            raise ProxyReloadError("No new proxies available.")
        if self.cfg.validation:
            new_proxies = self.validate_proxies(list(new_proxies))
        self.proxy_pool.update({proxy: (0, False) for proxy in new_proxies})
        self.logger.info(f"Reloaded proxy pool, {len(self.proxy_pool)} proxies available.")
        return self.get_available_proxy()

    def increment_proxy_usage(self, proxy: str) -> None:
        """Increments the usage count of a proxy."""
        if proxy in self.proxy_pool:
            usage, _ = self.proxy_pool[proxy]
            if usage < self.cfg.usage_limit:
                self.proxy_pool[proxy] = (usage + 1, True)
            else:
                self.remove_proxy_from_pool(proxy)
                raise UsageError(f"Proxy '{proxy}' has reached its usage limit")

    def release_proxy(self, proxy: str) -> None:
        """Releases a proxy back to the pool, making it available for use again."""
        if proxy in self.proxy_pool:
            usage, _ = self.proxy_pool[proxy]
            if usage < self.cfg.usage_limit:
                self.proxy_pool[proxy] = (usage, False)
            else:
                self.remove_proxy_from_pool(proxy)
