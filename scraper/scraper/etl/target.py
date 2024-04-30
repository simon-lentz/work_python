import pandas as pd
from typing import Optional
from pathlib import Path
from pydantic import field_validator, BaseModel, ConfigDict
from selenium.webdriver.remote.webdriver import WebDriver

from scraper.config.logging import StructuredLogger
from scraper.web.controller import WebController
from scraper.utils.exceptions import UsageError
from .extraction import Extraction, ExtractionManager
from .interaction import Interaction, InteractionManager


class TargetConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
    )

    name: str
    domain: str
    composite: bool
    input_file: Path
    input_data: Optional[bool] = None
    startup: Optional[list[Interaction]] = None
    interactions: Optional[list[Interaction]] = None
    extractions: Optional[list[Extraction]] = None

    @field_validator('input_file')
    def validate_input_file(cls, v):
        if not v.exists():
            raise ValueError(f"Input file '{v}' does not exist")
        return v


class TargetManager:

    def __init__(self, logger: StructuredLogger, controller: WebController, cfg):
        """
        Initializes the TargetManager with a logger, a WebController, and configuration data.

        :param logger: An instance of StructuredLogger for logging messages.
        :param controller: An instance of WebController for managing web interactions.
        :param cfg: Configuration data for the targets to be scraped.
        """
        self.logger = logger
        self.targets = cfg["target"]
        self.controller = controller
        self.retry_limit = 3

    def get_target_link(self, target: TargetConfig, link: str, retry_count: int = 0) -> None:
        """
        Attempts to make a request to the specified link for a target. If a UsageError occurs,
        retries the request with a rotated proxy and fresh startup actions, up to the retry limit.

        :param target: The target configuration object.
        :param link: The URL to make the request to.
        :param retry_count: The current count of retries for this request.
        """
        try:
            self.controller.make_request(target.name, link)
        except UsageError:
            if retry_count < self.retry_limit:
                self.logger.info(f"Proxy exhausted for {target.name}, rotating proxy and retrying...")
                connection = self.controller.get_connection(target.name)
                self.controller.rotate_proxy(connection)
                self.startup(target)  # redo startup actions with fresh connections
                self.get_target_link(target, link, retry_count + 1)
            else:
                self.logger.error(f"Exceeded retry limit for {target.name}, link: {link}")
                raise UsageError(f"Exceeded retry limit for {target.name}, link: {link}")
        except Exception as e:
            self.logger.error(f"Failed to make request for {target.name}, link: {link}: {e}", exc_info=True)

    def execute(self):
        """
        Executes the scraping process for each target specified in the configuration.
        """
        for target in self.targets:
            try:
                links, supplemental_data = self.read_input(target)
                self.scrape_target(target, links, supplemental_data)
            except Exception as e:
                self.logger.error(f"Failed to scrape {target}: {e}", exc_info=True)

    def scrape_target(self, target: TargetConfig, links: list, supplemental_data: list):
        """
        Scrapes each link for the specified target, performing interactions and extractions as defined.

        :param target: The target configuration object.
        :param links: A list of URLs to scrape.
        :param supplemental_data: A list of supplemental data corresponding to each link.
        """
        try:
            self.startup(target)
        except Exception as e:
            self.logger.error(f"Startup failed for {target.name}: {e}")
            return
        for idx, link in enumerate(links):
            try:
                self.get_target_link(target, link)
                connection = self.controller.get_connection(target.name)
                driver = connection.driver
                if target.interactions:
                    self.interactions(driver, target)
                self.extractions(driver, target, supplemental_data[idx])
            except Exception as e:
                self.logger.error(f"Failed to scrape {link}: {e}", exc_info=True)

    def startup(self, target: TargetConfig) -> None:
        """
        Performs the startup interactions for the specified target.

        :param target: The target configuration object.
        """
        if not target.startup:
            self.logger.info("No startup actions specified.")
            return
        try:
            self.get_target_link(target, target.domain)
            connection = self.controller.get_connection(target.name)
            driver = connection.driver
            startup_interactions = InteractionManager(self.logger, target.name)
            for interaction in target.startup:
                startup_interactions.perform_interaction(driver, interaction)
        except Exception as e:
            self.logger.error(f"Startup failed for {target.name}: {e}", exc_info=True)

    def interactions(self, driver: WebDriver, target: TargetConfig) -> None:
        """
        Performs the defined interactions for the specified target using the given WebDriver.

        :param driver: The WebDriver instance to use for interactions.
        :param target: The target configuration object.
        """
        interaction_manager = InteractionManager(self.logger, target.name)
        for interaction in target.interactions:
            try:
                interaction_manager.perform_interaction(driver, interaction)
            except Exception as e:
                self.logger.error(f"Failed to perform interaction {interaction}: {e}", exc_info=True)

    def extractions(self, driver: WebDriver, target: TargetConfig, supplemental_data: list) -> None:
        """
        Performs the defined extractions for the specified target using the given WebDriver.

        :param driver: The WebDriver instance to use for extractions.
        :param target: The target configuration object.
        :param supplemental_data: Supplemental data corresponding to the current extraction.
        """
        if target.composite:
            extraction_manager = ExtractionManager(self.logger, target.name, self.controller)
        else:
            extraction_manager = ExtractionManager(self.logger, target.name)
        for extraction in target.extractions:
            try:
                if extraction.pagination_locator:
                    data = extraction_manager.perform_paginated_extraction(driver, extraction, supplemental_data)
                else:
                    data = extraction_manager.perform_extraction(driver, extraction, supplemental_data)
            except Exception as e:
                self.logger.error(f"Extraction {extraction} failed: {e}", exc_info=True)

            try:
                self.write_output(target.name, data, extraction.output_type, extraction.output_file)
            except Exception as e:
                self.logger.error(f"Failed to write data {data} to output {extraction.output_file}: {e}", exc_info=True)

    def read_input(self, target: TargetConfig) -> tuple:
        """
        Reads the input file for the specified target, returning a tuple of links and supplemental data.

        :param target: The target configuration object.
        :return: A tuple containing a list of links and a list of supplemental data.
        """
        input_file = target.input_file
        try:
            with open(input_file, "r") as file:
                input_data = [line.strip().split(',') for line in file]
            links = [data[0] for data in input_data]
            supplemental_data = [data[1:] for data in input_data]
            return links, supplemental_data
        except FileNotFoundError as e:
            raise OSError(f"Failed to read links from input TXT file '{input_file}' for '{target.name}': {e}")

    def write_output(self, name: str, data: list, output_type: str, output_file: Path):
        """
        Writes the extracted data to an output file in the specified format.

        :param name: The name of the target.
        :param data: The extracted data to write.
        :param output_type: The type of output file (e.g., 'csv').
        :param output_file: The path to the output file.
        """
        if not data:
            self.logger.info(f"No data to write for output file: {output_file}")
            return
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(data)
        df = df.dropna()
        if df.empty:
            self.logger.info(f"No valid data to write {data}")
            return
        output_writers = {
            "csv": lambda: df.to_csv(output_file, mode='a', index=False, header=False),
        }
        output_type = output_type.lower()
        if output_type in output_writers:
            output_writers[output_type]()
            self.logger.info(f"Output for '{name}' written to {output_type.upper()}: {output_file}")
        else:
            self.logger.error(f"Unsupported output type for '{name}': '{output_type}'")
