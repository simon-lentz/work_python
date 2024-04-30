from enum import Enum
from pydantic import BaseModel, ConfigDict
from typing import Optional
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException,
    ElementNotInteractableException, ElementClickInterceptedException,
)

from scraper.config.logging import StructuredLogger
from scraper.utils.exceptions import ClickException, DropdownSelectionException
from .funcs import get_element, parse_locator


class InteractionType(str, Enum):
    """Enum representing the types of interactions that can be performed."""
    CLICK = "click"
    DROPDOWN = "dropdown"


class Interaction(BaseModel):
    """Represents an interaction to be performed on a web page."""
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
    )

    type: InteractionType
    locator: str
    locator_type: str
    wait_interval: float = 0.5
    option_text: Optional[str] = None


class InteractionManager:
    """Manages the execution of interactions on a web page."""

    def __init__(self, logger: StructuredLogger, target_name: str):
        """
        Initializes the InteractionManager with a logger and the name of the target.

        :param logger: An instance of StructuredLogger for logging messages.
        :param target_name: The name of the target for which interactions are managed.
        """
        self.logger = logger
        self.name = target_name

    def perform_interaction(self, driver: WebDriver, interaction: Interaction) -> None:
        """
        Performs a single interaction on the web page.

        :param driver: The WebDriver instance to use for interactions.
        :param interaction: The interaction object defining the interaction to be performed.
        """
        try:
            match interaction.type:
                case InteractionType.CLICK:
                    self.click(driver, interaction)
                    self.logger.info(f"Clicked on element: {interaction.locator}")
                case InteractionType.DROPDOWN:
                    self.dropdown(driver, interaction)
                    self.logger.info(f"Selected {interaction.option_text} from dropdown: {interaction.locator}")
                case _:
                    self.logger.error(f"Undefined interaction {interaction.type}")
        except Exception as e:
            self.logger.error(f"Exception raised during {interaction.type} interaction for {self.name}: {e}", exc_info=True)  # noqa:E501

    def click(self, driver: WebDriver, interaction: Interaction) -> None:
        """
        Performs a click interaction on the web page.

        :param driver: The WebDriver instance to use for interactions.
        :param interaction: The interaction object defining the click interaction to be performed.
        """
        try:
            locator, by_type = parse_locator(interaction.locator, interaction.locator_type)
            element = get_element(driver, locator, by_type, interaction.wait_interval)
            element.click()
        except (NoSuchElementException, ElementNotInteractableException, ElementClickInterceptedException):
            try:
                element = WebDriverWait(driver, interaction.wait_interval, 0.01).until(
                    EC.element_to_be_clickable((by_type, locator))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                element.click()
            except TimeoutException:
                try:
                    # Retry with an action chain if the element is not interactable
                    ActionChains(driver).move_to_element(element).click(element).perform()
                except Exception as e:
                    raise ClickException(f"Failed to click on element {interaction.locator}: {e}")

    def dropdown(self, driver: WebDriver, interaction: Interaction) -> None:
        """
        Performs a dropdown selection interaction on the web page.

        :param driver: The WebDriver instance to use for interactions.
        :param interaction: The interaction object defining the dropdown selection to be performed.
        """
        try:
            locator, by_type = parse_locator(interaction.locator, interaction.locator_type)
            element = get_element(driver, locator, by_type, interaction.wait_interval)
            select = Select(element)
            select.select_by_visible_text(interaction.option_text)
        except (NoSuchElementException, ElementNotInteractableException):
            try:
                element = WebDriverWait(driver, interaction.wait_interval, 0.01).until(
                    EC.element_to_be_clickable((by_type, locator))
                )
                select = Select(element)
                select.select_by_visible_text(interaction.option_text)
            except TimeoutException:
                try:
                    # Retry with an action chain if the element is not interactable
                    action_chain = ActionChains(driver)
                    action_chain.move_to_element(element).click().perform()
                    select = Select(element)
                    select.select_by_visible_text(interaction.option_text)
                except Exception as e:
                    raise DropdownSelectionException(f"Failed to select {interaction.option_text} from dropdown {interaction.locator}: {e}")  # noqa:E501
