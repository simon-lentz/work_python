from datetime import datetime
from typing import Optional
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from scraper.utils.exceptions import ElementNotFoundException, LocatorTypeException


def click_and_wait_for_tab(driver, li_selector, div_selector, timeout=10):
    # Click the <li> element.
    li_element = driver.find_element(By.CSS_SELECTOR, li_selector)
    li_element.click()

    # Wait for the <div> element to be loaded.
    div_element = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, div_selector))
    )

    return div_element


def paginate_tab(div_element: WebElement) -> bool:
    """
    Attempts to paginate to the next page within a tab.

    Parameters:
    - div_element: The WebElement containing the pagination controls.

    Returns:
    - bool: True if pagination was successful, False otherwise.
    """
    try:
        # Find the "Next" button using the specific selector for your pagination controls.
        next_page_button = div_element.find_element(By.CSS_SELECTOR, ".paginate_button.next")

        # Check if the "Next" button is disabled.
        if "disabled" in next_page_button.get_attribute("class"):
            return False

        # Click the "Next" button if it's not disabled.
        next_page_button.click()
        return True
    except NoSuchElementException:
        # If the "Next" button is not found, return False.
        return False


def get_element(driver: WebDriver, locator: str, by_type: By, wait_interval: float) -> WebElement:
    """
    Attempts to find an element immediately, then retries with an explicit wait if not found.

    :param driver: The WebDriver instance to use for finding elements.
    :param locator: The locator string used to identify the element.
    :param by_type: The type of locator (e.g., By.ID, By.XPATH).
    :param wait_interval: The maximum time to wait for the element to be found.
    :return: The WebElement found using the provided locator.
    :raises ElementNotFoundException: If the element cannot be found within the wait interval.
    """
    try:
        return driver.find_element(by_type, locator)
    except NoSuchElementException:
        try:
            return WebDriverWait(driver, wait_interval, 0.01).until(
                EC.presence_of_element_located((by_type, locator))
            )
        except TimeoutException as e:
            raise ElementNotFoundException(f"Element {locator} not found via {by_type}: {e}")


def parse_locator(locator: str, locator_type: str) -> tuple:
    """
    Converts a string locator type into a corresponding Selenium By object.

    :param locator: The locator string used to identify the element.
    :param locator_type: The type of locator as a string (e.g., "id", "xpath").
    :return: A tuple containing the locator string and the corresponding Selenium By object.
    :raises LocatorTypeException: If the provided locator type is not supported.
    """
    formatted_strategy = locator_type.strip().replace(" ", "_").upper()
    match formatted_strategy:
        case "ID":
            return locator, By.ID
        case "XPATH":
            return locator, By.XPATH
        case "LINK_TEXT":
            return locator, By.LINK_TEXT
        case "PARTIAL_LINK_TEXT":
            return locator, By.PARTIAL_LINK_TEXT
        case "NAME":
            return locator, By.NAME
        case "TAG_NAME":
            return locator, By.TAG_NAME
        case "CLASS_NAME":
            return locator, By.CLASS_NAME
        case "CSS_SELECTOR":
            return locator, By.CSS_SELECTOR
        case _:
            raise LocatorTypeException(f"Unsupported Selenium By-type {formatted_strategy}")


def paginate(driver: WebDriver, locator: str, by_type: By, wait_interval: float) -> tuple[int, Optional[WebElement]]:
    """
    Extracts the maximum number of pages from a pagination element and attempts to click the "Next" button
    to load the next page.

    :param driver: The WebDriver instance to use for pagination.
    :param locator: The locator string used to identify the pagination element.
    :param by_type: The type of locator (e.g., By.ID, By.XPATH) for the pagination element.
    :param wait_interval: The maximum time to wait for the pagination element to be found.
    :return: A tuple containing the maximum number of pages and the WebElement for the "Next" button, if available.
    """
    pagination_element = get_element(driver, locator, by_type, wait_interval)
    pagination_html = pagination_element.get_attribute('innerHTML')
    soup = BeautifulSoup(pagination_html, 'html.parser')
    # Get the maximum number of pages
    page_numbers = [int(link.text) for link in soup.find_all('a', class_='paginate_button') if link.text.isdigit()]
    max_pages = max(page_numbers) if page_numbers else 1
    # Attempt to click the "Next" button
    try:
        next_button = pagination_element.find_element(By.CSS_SELECTOR, "a.paginate_button.next")
        if "disabled" in next_button.get_attribute("class"):
            return max_pages, None  # "Next" button is disabled, indicating the last page
        return max_pages, next_button
    except NoSuchElementException:
        try:
            next_button = WebDriverWait(driver, wait_interval, 0.01).until(
                EC.element_to_be_clickable((by_type, locator))
            )
            return max_pages, next_button
        except TimeoutException:
            return max_pages, None


def sanitize_data(data: str, invalid_output: list[str]) -> str:
    """
    Cleans extracted data by removing unwanted characters and checking against invalid values.

    :param data: The extracted data to be cleaned.
    :param invalid_output: A list of strings considered invalid and to be replaced with None.
    :return: The cleaned data.
    """
    cleaned_data = data.replace(",", "").replace("\t", " ").replace("\n", " ").replace("\r", "")
    if cleaned_data in invalid_output:
        return None
    return cleaned_data


def timestamp() -> str:
    """
    Returns the current date and time as a string.

    :return: The current date and time formatted as "%m/%d/%Y".
    """
    now = datetime.now()
    return now.strftime("%m/%d/%Y")
