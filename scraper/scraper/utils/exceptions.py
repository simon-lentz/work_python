class ElementNotFoundException(Exception):
    """Raised when an expected element is not found on a web page."""


class DropdownSelectionException(Exception):
    """Raised when a dropdown selection fails or an option is not found."""


class ClickException(Exception):
    """Raised when a click action on an element fails."""


class ParseElementException(Exception):
    """Raised when parsing an element's content encounters an error."""


class ParseTableException(Exception):
    """Raised when parsing a table element encounters an error."""


class LocatorTypeException(Exception):
    """Raised when an unsupported locator type is specified for finding elements."""


class OCRException(Exception):
    """Raised when an Optical Character Recognition (OCR) process fails."""


class UsageError(Exception):
    """Raised when usage exceeds a specified usage limit."""


class ProxyReloadError(Exception):
    """Raised when reloading the proxy pool fails."""


class ConfigError(Exception):
    """Raised for errors related to configuration files or settings."""
