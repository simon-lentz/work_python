from enum import Enum
from pathlib import Path
from typing import Optional
from bs4 import BeautifulSoup
from pydantic import BaseModel, ConfigDict
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from .ocr import rating_ocr, cusip_ocr
from .funcs import parse_locator, paginate_tab, click_and_wait_for_tab, get_element, paginate, sanitize_data, timestamp
from scraper.web.controller import WebController
from scraper.config.logging import StructuredLogger
from scraper.utils.exceptions import ParseTableException, ParseElementException


class OutputType(str, Enum):
    """Enum representing supported output file types."""
    CSV = "csv"
    JSON = "json"
    TXT = "txt"
    PANDAS = "pandas"


class ExtractionType(str, Enum):
    """Enum representing the types of extractions that can be performed."""
    ELEMENT = "element"
    ISSUER_TABLE = "issuer table"
    ISSUE_SCALE_TABLE = "issue scale table"
    ISSUE_OS_TABLE = "issue os table"
    SOURCE = "source"


class Extraction(BaseModel):
    """Represents an extraction to be performed on a web page."""
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
    )

    type: ExtractionType
    locator: str
    locator_type: str
    wait_interval: float = 0.5
    pagination_locator: Optional[str] = None
    pagination_locator_type: Optional[str] = None
    exclude_tags: Optional[dict[str, list[str]]] = None
    output_type: OutputType
    output_file: Path
    invalid_output: Optional[list[str]] = None


class ExtractionManager:
    """Manages the extraction process for web scraping."""

    def __init__(self, logger: StructuredLogger, target_name: str, controller: Optional[WebController] = None):
        """
        Initializes the ExtractionManager with a logger, the name of the target, and optionally a WebController.

        :param logger: An instance of StructuredLogger for logging messages.
        :param target_name: The name of the target for which extractions are managed.
        :param controller: An optional instance of WebController for managing web interactions.
        """
        self.logger = logger
        self.name = target_name
        self.controller = controller

    def perform_extraction(self, driver: WebDriver, extraction: Extraction, supplemental_data: list) -> list:
        """
        Extracts data based on the provided extraction configuration.

        :param driver: The WebDriver instance to use for extractions.
        :param extraction: The extraction object defining the extraction to be performed.
        :param supplemental_data: Supplemental data to be appended to the extracted data.
        :return: A list containing the extracted data.
        """
        try:
            locator, by_type = parse_locator(extraction.locator, extraction.locator_type)
            match extraction.type:
                case ExtractionType.ELEMENT:
                    element = get_element(driver, locator, by_type, extraction.wait_interval)
                    return [self.parse_element(element, extraction)]
                case ExtractionType.ISSUER_TABLE:
                    element = get_element(driver, locator, by_type, extraction.wait_interval)
                    return self.issuer_table(element, extraction, supplemental_data)
                case ExtractionType.ISSUE_SCALE_TABLE:
                    element = get_element(driver, locator, by_type, extraction.wait_interval)
                    return self.issue_scale_table(element, extraction, supplemental_data)
                case ExtractionType.ISSUE_OS_TABLE:
                    element = click_and_wait_for_tab(driver, "li[data-cid='t-os']", "div#t-os[style='']")
                    return self.issue_os_table(element, extraction, supplemental_data)
                case ExtractionType.SOURCE:
                    return [str(driver.page_source)]
                case _:
                    self.logger.error(f"Undefined extraction '{extraction.type}'")
            return []
        except Exception as e:
            self.logger.error(f"Failed to perform extraction for {self.name}: {e}", exc_info=True)

    def perform_paginated_extraction(self, driver: WebDriver, extraction: Extraction, supplemental_data: list) -> list:
        """
        Extracts data from paginated web pages based on the provided extraction configuration.

        :param driver: The WebDriver instance to use for extractions.
        :param extraction: The extraction object defining the extraction to be performed.
        :param supplemental_data: Supplemental data to be appended to the extracted data.
        :return: A list containing the extracted data from all pages.
        """
        all_data = []
        page_count = 0
        locator, by_type = parse_locator(extraction.pagination_locator, extraction.pagination_locator_type)
        max_pages, next_button = paginate(driver, locator, by_type, extraction.wait_interval)
        while page_count < max_pages:
            page_data = self.perform_extraction(driver, extraction, supplemental_data)
            all_data.extend(page_data)
            page_count += 1
            if next_button is not None:
                next_button.click()
                _, next_button = paginate(driver, locator, by_type, extraction.wait_interval)  # Update next_button
            else:
                break
            self.logger.info(f"Paginating, current page: {page_count}")
        return all_data

    def parse_element(self, element: WebElement, extraction: Extraction) -> list[str]:
        """
        Parses the text content of a WebElement, optionally excluding certain tags, and cleans the extracted data.

        :param element: The WebElement to parse.
        :param extraction: The extraction object defining the parsing and cleaning options.
        :return: A list containing the cleaned text content of the WebElement.
        """
        try:
            html = element.get_attribute('innerHTML')
            soup = BeautifulSoup(html, 'html.parser')
            if extraction.exclude_tags:
                for tag in extraction.exclude_tags:
                    for match in soup.find_all(tag):
                        match.decompose()
            text = soup.get_text(separator="&&&", strip=True)
            return [sanitize_data(entry, extraction.invalid_output) for entry in text.split("&&&") if entry.strip()]
        except Exception as e:
            raise ParseElementException(f"Failed to parse element: {e}")

    def issue_scale_table(self, element: WebElement, extraction: Extraction, supplemental_data: list) -> list[list[str]]:  # noqa:E501
        """
        Parses an HTML table into a list of lists, optionally excluding certain tags or attributes.

        :param element: The WebElement representing the HTML table to parse.
        :param extraction: The extraction object defining the parsing and cleaning options.
        :param supplemental_data: Supplemental data to be appended to each row of the extracted table data.
        :return: A list of lists containing the cleaned data from the HTML table.
        """
        try:
            rows_data = []
            html = element.get_attribute('outerHTML')
            soup = BeautifulSoup(html, 'html.parser')
            if extraction.exclude_tags:
                for tag, attrs in extraction.exclude_tags.items():
                    for match in soup.find_all(tag):
                        for attr in attrs:
                            if match.has_attr(attr):
                                match.decompose()
            rows = soup.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                row_data = []
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    if cell_text:
                        row_data.append(sanitize_data(cell_text, extraction.invalid_output))
                    a_tag = cell.find('a', href=True)
                    if a_tag:
                        cusip_link = "https://emma.msrb.org" + a_tag['href']
                        cusip_image = a_tag.find('img', src=True)
                        cusip_ocr_link = "https://emma.msrb.org" + cusip_image['src']
                        if self.controller is not None:
                            ocr_connection = self.controller.get_connection(f"{self.name}_composite")
                            ocr_driver = ocr_connection.driver
                            cusip = cusip_ocr(self.logger, cusip_ocr_link, ocr_driver)
                            row_data.append(cusip)
                            row_data.append(cusip_link)
                            split_link = cusip_link.split('/')
                            row_data.append(split_link[-1])
                        else:
                            row_data.append(cusip_ocr_link)
                            row_data.append(cusip_link)
                    rating_tag = cell.find('img', src=True, attrs={"data-rating": True})
                    if rating_tag:
                        rating_ocr_link = "https://emma.msrb.org" + rating_tag['src']
                        if self.controller is not None:
                            ocr_connection = self.controller.get_connection(f"{self.name}_composite")
                            ocr_driver = ocr_connection.driver
                            rating = rating_ocr(self.logger, rating_ocr_link, ocr_driver)
                            row_data.append(rating)
                        else:
                            row_data.append(rating_ocr_link)
                row_data.append(timestamp())
                row_data.extend(supplemental_data)
                rows_data.append(row_data)
            return rows_data
        except Exception as e:
            raise ParseTableException(f"Failed to parse table: {e}")

    def issuer_table(self, element: WebElement, extraction: Extraction, supplemental_data: list) -> list[list[str]]:  # noqa:E501
        """
        Parses an HTML table into a list of lists, optionally excluding certain tags or attributes.

        :param element: The WebElement representing the HTML table to parse.
        :param extraction: The extraction object defining the parsing and cleaning options.
        :param supplemental_data: Supplemental data to be appended to each row of the extracted table data.
        :return: A list of lists containing the cleaned data from the HTML table.
        """
        try:
            rows_data = []
            html = element.get_attribute('outerHTML')
            soup = BeautifulSoup(html, 'html.parser')
            if extraction.exclude_tags:
                for tag, attrs in extraction.exclude_tags.items():
                    for match in soup.find_all(tag):
                        for attr in attrs:
                            if match.has_attr(attr):
                                match.decompose()
            rows = soup.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                row_data = []
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    if cell_text:
                        row_data.append(sanitize_data(cell_text, extraction.invalid_output))
                    a_tag = cell.find('a', href=True)
                    if a_tag:
                        issue_link = "https://emma.msrb.org" + a_tag['href']
                        row_data.append(issue_link)
                row_data.append(timestamp())
                row_data.extend(supplemental_data)
                rows_data.append(row_data)
            return rows_data
        except Exception as e:
            raise ParseTableException(f"Failed to parse table: {e}")

    def issue_os_table(self, div_element: WebElement, extraction: Extraction, supplemental_data: list) -> list[list[str]]:  # noqa:E501
        """
        Parses an HTML table into a list of lists, optionally excluding certain tags or attributes, and handles pagination.

        :param div_element: The html element the represents the div tab that contains the rendered table.
        :param extraction: The extraction object defining the parsing and cleaning options.
        :param supplemental_data: Supplemental data to be appended to each row of the extracted table data.
        :return: A list of lists containing the cleaned data from the HTML table.
        """  # noqa:E501
        try:
            all_data = []
            while True:
                rows_data = []
                html = div_element.get_attribute('outerHTML')
                soup = BeautifulSoup(html, 'html.parser')
                rows = soup.find_all('tr')
                for row in rows:
                    row_data = []
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        a_tag = cell.find('a', href=True)
                        if a_tag:
                            link = "https://emma.msrb.org" + a_tag['href']
                            row_data.append(link)
                    row_data.append(timestamp())
                    row_data.extend(supplemental_data)
                    if len(row_data) == 7:
                        rows_data.append(row_data)
                all_data.extend(rows_data)

                # Attempt to paginate to the next page
                if not paginate_tab(div_element):
                    break  # Exit the loop if there are no more pages to paginate

                self.logger.info("paginating...")

            return all_data
        except Exception as e:
            raise ParseTableException(f"Failed to parse table: {e}")
