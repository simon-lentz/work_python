import requests
import concurrent.futures
from pathlib import Path
from typing import Optional
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

    def validate_proxies(self, proxies: list[str]) -> list[str]:
        """Validates the list of proxies by testing their connectivity."""
        valid_proxies = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_proxy = {executor.submit(self.is_proxy_valid, proxy): proxy for proxy in proxies}
            for future in concurrent.futures.as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                if future.result():
                    valid_proxies.append(proxy)
        return valid_proxies

    def is_proxy_valid(self, proxy: str) -> bool:
        """Checks if a proxy is valid by testing its connectivity."""
        proxy_url = self.format_proxy_url(proxy)
        try:
            response = requests.get(self.cfg.test_url, proxies={"http": proxy_url, "https": proxy_url}, timeout=5)
            return response.status_code == 200
        except (requests.RequestException, Exception) as e:
            self.logger.warning(f"Proxy validation error: {e}")
            return False

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
