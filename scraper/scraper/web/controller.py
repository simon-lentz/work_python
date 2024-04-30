import time
from selenium.common.exceptions import TimeoutException
from scraper.config.logging import StructuredLogger
from scraper.utils.connection import ConnectionData
from scraper.utils.exceptions import UsageError
from scraper.web.docker import DockerManager
from scraper.web.driver import DriverManager
from scraper.web.proxy import ProxyManager


class WebController:
    def __init__(self, logger: StructuredLogger, cfg) -> None:
        self.logger = logger
        # Create connection data dict
        self.connections = self.create_connections(cfg)
        # After assigning connections, initialize managers
        self.docker_manager = DockerManager(self.logger, cfg["docker"])
        self.driver_manager = DriverManager(self.logger, cfg["driver"])
        self.proxy_manager = ProxyManager(self.logger, cfg["proxy"])
        self.connected = False

    def __enter__(self):
        self.connect()
        self.connected = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connected:
            self.disconnect()

    def create_connections(self, cfg) -> dict[str, ConnectionData]:
        connections: dict[str, ConnectionData] = {}
        for target in cfg["target"]:
            try:
                port = cfg["docker"].ports.pop(0)  # Assume ports are assigned in order
                connections[target.name] = ConnectionData(target.name, port)
                if target.composite:
                    port = cfg["docker"].ports.pop(0)
                    composite_name = f"{target.name}_composite"
                    connections[composite_name] = ConnectionData(composite_name, port)
            except Exception as e:
                self.logger.error(f"Failed to initialize ConnectionData for target {target}: {e}", exc_info=True)
        return connections

    def connect_container(self, connection: ConnectionData) -> None:
        try:
            container = self.docker_manager.create_container(connection)
            connection.container = container
        except Exception as e:
            self.logger.error(f"Failed to connect container for '{connection.name}': {e}", exc_info=True)

    def connect_driver(self, connection: ConnectionData) -> None:
        try:
            driver = self.driver_manager.create_driver(connection)
            connection.driver = driver
        except Exception as e:
            self.logger.error(f"Failed to connect driver for '{connection.name}': {e}", exc_info=True)

    def get_connection(self, target_name: str) -> ConnectionData:
        try:
            return self.connections.get(target_name)
        except Exception as e:
            self.logger.error(f"No connection found for target {target_name}: {e}", exc_info=True)

    def connect(self) -> None:
        """Attempts to connect all necessary resources for each target."""
        for target, connection in self.connections.items():
            if not self.connect_resources(connection):
                self.logger.error(f"Failed to fully establish resources for target '{target}'.")

    def connect_resources(self, connection: ConnectionData) -> bool:
        """Attempts to connect all required resources and logs any failures."""
        successful = True
        operations = [
            (self.proxy_manager.get_available_proxy, "proxy"),
            (lambda: self.connect_container(connection), "container"),
            (lambda: self.connect_driver(connection), "driver")
        ]

        for operation, resource in operations:
            try:
                result = operation()
                if resource == "proxy":
                    connection.proxy = result
            except Exception as e:
                self.logger.error(f"Failed to assign {resource} for connection '{connection.name}': {e}")
                successful = False

        return successful

    def disconnect(self):
        """Safely disconnects all resources for each connection."""
        for target, connection in self.connections.items():
            try:
                self.release_resources(connection)
            except KeyboardInterrupt:
                self.logger.error(f"Keyboard interrupt during disconnect of {target}")
                continue  # Ignore repeated interrupts
            except Exception as e:
                self.logger.error(f"Failed to release resources for {target}: {e}")

    def release_resources(self, connection: ConnectionData):
        """Releases resources associated with a given connection, handling errors gracefully."""
        if hasattr(connection, 'driver') and connection.driver:
            try:
                self.driver_manager.quit_driver(connection.driver)
            except Exception as e:
                self.logger.error(f"Error quitting driver for {connection.name}: {e}")

        if hasattr(connection, 'container') and connection.container:
            try:
                self.docker_manager.cleanup(connection.container)
            except Exception as e:
                self.logger.error(f"Error cleaning up container for {connection.name}: {e}")

        if hasattr(connection, 'proxy') and connection.proxy:
            try:
                self.proxy_manager.release_proxy(connection.proxy)
            except Exception as e:
                self.logger.error(f"Error releasing proxy for {connection.name}: {e}")

    def make_request(self, target_name: str, url: str) -> None:
        connection = self.get_connection(target_name)
        driver = connection.driver
        max_retries = 2  # Allows for a total of 3 attempts
        attempt = 0

        while attempt <= max_retries:
            try:
                driver.get(url)
                self.proxy_manager.increment_proxy_usage(connection.proxy)
                return  # Successful get, break out of the loop
            except TimeoutException as e:
                self.logger.warning(f"Timeout on attempt {attempt + 1} for URL '{url}': {e}")
                if attempt < max_retries:
                    time.sleep((attempt + 1) * 2)  # Progressive delay: 2, 4, etc. seconds
                    attempt += 1
                else:
                    self.logger.error(f"Final attempt failed for URL '{url}': {e}")
                    raise TimeoutException(f"Request to '{url}' failed after {max_retries + 1} attempts.")
            except UsageError as e:
                raise e  # Reraise UsageError without retry
            except Exception as e:
                self.logger.error(f"Non-timeout error occurred for URL '{url}': {e}")
                raise  # Reraise the exception if it's not a TimeoutException

    def rotate_proxy(self, connection: ConnectionData) -> None:
        try:
            new_proxy = self.proxy_manager.get_available_proxy()
            connection.proxy = new_proxy
        except Exception as e:
            self.logger.error(f"Failed to rotate proxy for connection '{connection.name}': {e}", exc_info=True)

        try:
            self.driver_manager.quit_driver(connection.driver)
            new_driver = self.driver_manager.create_driver(connection)
            connection.driver = new_driver
            self.logger.info(f"'{connection.name}' rotated proxy")
        except Exception as e:
            self.logger.error(f"Failed to assign new driver to {connection.name} connection: {e}", exc_info=True)
