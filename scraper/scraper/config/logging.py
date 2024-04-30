import os
import logging
import json

from pathlib import Path
from datetime import datetime
from pydantic import field_validator, BaseModel, ConfigDict, Field


class LoggingConfig(BaseModel):
    """Pydantic model for logging configuration."""
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_to_lower=True,
        str_strip_whitespace=True,
        str_min_length=1,
    )

    log_directory: Path
    log_level: str
    log_format: str
    log_max_size: str = Field(..., pattern=r"^\d+[KMGBkmgb][Bb]?$")

    @field_validator("log_level")
    @classmethod
    def check_valid_log_level(cls, v: str) -> str:
        level = v.upper()
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if level in valid_levels:
            return level
        else:
            raise ValueError(f"Log level '{v}' is not supported. Valid levels are: {', '.join(valid_levels)}")

    @field_validator("log_directory")
    @classmethod
    def check_directory_exists(cls, v: Path) -> Path:
        if v.exists() and v.is_dir():
            return v
        else:
            raise ValueError(f"Log file directory '{v}' not found")


class JsonFormatter(logging.Formatter):
    """Formats log records to JSON."""

    def format(self, record: logging.LogRecord) -> str:
        """Formats a log record into a JSON string."""
        log_record = {
            "time": datetime.fromtimestamp(record.created).isoformat(),
            "name": record.name,
            "level": record.levelname,
            "origin": f"{record.module}.{record.funcName}, line {record.lineno}",
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record, indent=4) + ","


class StructuredLogger(logging.Logger):
    """Extension of stdlib logging.Logger, outputs structured logs in JSON format."""

    def __init__(self, target_type: str, cfg: LoggingConfig):
        self.cfg = cfg
        super().__init__(name=f"Scraper Log: {target_type}", level=self.cfg.log_level)
        self.log_max_size = self.parse_log_max_size()
        self.logger = logging.getLogger(target_type)
        self.configure_file_handler()

    def parse_log_max_size(self) -> int:
        """Parses the log file size limit from a string to an integer in bytes."""
        size_map = {"b": 8, "k": 1024, "kb": 1024, "m": 1024**2, "mb": 1024**2, "g": 1024**3, "gb": 1024**3}
        cfg_val = self.cfg.log_max_size.lower()
        # Extract the numeric part and the unit part from the string
        size = int("".join(filter(str.isdigit, cfg_val)))
        unit = "".join(filter(str.isalpha, cfg_val))
        if unit in size_map:
            return size * size_map[unit]
        else:
            raise ValueError(f"Invalid log size unit '{unit}' in '{self.cfg.log_max_size}'. Valid units are: {', '.join(size_map.keys())}")  # noqa:E501

    def configure_file_handler(self):
        """Configures the file handler."""
        self.log_file = self.cfg.log_directory / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_handler = logging.FileHandler(self.log_file, mode="a")
        file_handler.setLevel(self.cfg.log_level.upper())
        file_handler.setFormatter(JsonFormatter())
        self.addHandler(file_handler)
        with open(self.log_file, "w") as f:
            f.write("[")

    def rotate_log_file(self):
        """Rotates the log file when it reaches or exceeds the size limit."""
        self.close()  # Close the current log file
        self.configure_file_handler()  # Reconfigure the file handler

    def handle(self, record):
        """Handles a log record, rotating the log file if necessary."""
        if os.path.getsize(self.log_file) >= self.log_max_size:
            self.rotate_log_file()
        super().handle(record)

    def close(self):
        """Closes the logger and its handlers, properly finalizing the log file."""
        with open(self.log_file, "rb+") as f:
            f.seek(0, 2)  # Move the cursor to the end of the file
            size = f.tell()
            if size >= 2:
                f.seek(-2, 2)  # Move the cursor to final curly bracket
                f.truncate()  # Remove the last comma and newline
                f.write(b"]")  # Write the closing bracket
            else:
                f.write(b"]")  # if file is empty, close array in-place
        for handler in self.handlers:
            handler.close()
            self.removeHandler(handler)
