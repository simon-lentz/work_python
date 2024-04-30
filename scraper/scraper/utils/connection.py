from typing import Optional
from docker.models.containers import Container
from selenium.webdriver.remote.webdriver import WebDriver


class ConnectionData:
    """
    Represents the data associated with a web scraping connection.

    This class stores information about a connection, including its name, port,
    proxy, Docker container, and WebDriver. The properties for container, driver,
    and proxy include validation to ensure that they are set to appropriate values.

    Attributes:
        name (str): The name of the connection.
        port (str): The port on which the connection is established.
        proxy (Optional[str]): The proxy used by the connection.
        container (Optional[Container]): The Docker container associated with the connection.
        driver (Optional[WebDriver]): The WebDriver instance used by the connection.
    """  # noqa:E501

    def __init__(self, name: str, port: str,
                 proxy: Optional[str] = None, container: Optional[Container] = None,
                 driver: Optional[WebDriver] = None):
        """
        Initializes a new instance of the ConnectionData class.

        Args:
            name (str): The name of the connection.
            port (str): The port on which the connection is established.
            proxy (Optional[str]): The proxy to be used by the connection. Defaults to None.
            container (Optional[Container]): The Docker container associated with the connection. Defaults to None.
            driver (Optional[WebDriver]): The WebDriver instance used by the connection. Defaults to None.
        """  # noqa:E501
        self.name = name
        self.port = port
        self._proxy = proxy
        self._container = container
        self._driver = driver

    @property
    def container(self) -> Container:
        """
        The Docker container associated with the connection.

        Raises:
            ValueError: If the container has not been set.
        """
        if isinstance(self._container, Container):
            return self._container
        else:
            raise ValueError(f"Container not set for connection '{self.name}'")

    @container.setter
    def container(self, container: Container):
        """
        Sets the Docker container for the connection.

        Args:
            container (Container): The Docker container to associate with the connection.

        Raises:
            TypeError: If the provided value is not a Container instance.
        """  # noqa:E501
        if not isinstance(container, Container):
            raise TypeError(f"Expected 'Container', got '{type(container).__name__}'")
        self._container = container

    @property
    def driver(self) -> WebDriver:
        """
        The WebDriver instance associated with the connection.

        Raises:
            ValueError: If the driver has not been set.
        """
        if isinstance(self._driver, WebDriver):
            return self._driver
        else:
            raise ValueError(f"Driver not set for connection '{self.name}'")

    @driver.setter
    def driver(self, driver: WebDriver):
        """
        Sets the WebDriver for the connection.

        Args:
            driver (WebDriver): The WebDriver instance to associate with the connection.

        Raises:
            TypeError: If the provided value is not a WebDriver instance.
        """
        if not isinstance(driver, WebDriver):
            raise TypeError(f"Expected 'WebDriver', got '{type(driver).__name__}'")
        self._driver = driver

    @property
    def proxy(self) -> str:
        """
        The proxy used by the connection.

        Raises:
            ValueError: If the proxy has not been set.
        """
        if self._proxy is not None:
            return self._proxy
        else:
            raise ValueError(f"Proxy not set for connection '{self.name}'")

    @proxy.setter
    def proxy(self, proxy: str):
        """
        Sets the proxy for the connection.

        Args:
            proxy (str): The proxy to use for the connection.

        Raises:
            ValueError: If the provided value is not a valid proxy string.
        """
        if not isinstance(proxy, str):
            raise ValueError(f"Invalid proxy '{proxy}'")
        self._proxy = proxy
