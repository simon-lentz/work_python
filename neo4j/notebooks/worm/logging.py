import logging

# Configure logging
logger = logging.getLogger("Neo4j ORM")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

"""
This module configures the global logging setup for the Neo4j ORM application.

It sets up a logger named 'Neo4j ORM' with an INFO level threshold. The logger outputs
to the console and formats the log messages to include the timestamp, logger name, level of severity,
and the message. This configuration is intended for tracking the flow of the application and diagnosing
issues by logging informative messages about the application's operations.

Example Usage:
    from .logging import logger

    logger.info("This is an informational message")
    logger.error("This is an error message")
"""
