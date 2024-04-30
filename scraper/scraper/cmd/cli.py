import argparse
from pathlib import Path

from scraper.config.validator import load_config
from scraper.config.logging import StructuredLogger
from scraper.utils.profiling import RuntimeProfile


def parse_arguments():
    """Parses command line arguments for the web scraper."""
    parser = argparse.ArgumentParser(description="Web Scraper")
    parser.add_argument(
        "--target-type",
        type=str,
        help="Specify the target type (or leave blank for example runtime)",
        required=True
    )
    parser.add_argument(
        "--config-format",
        type=str,
        default="yaml",
        help="Defaults to YAML, also supports: JSON, TOML",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--profiling",
        action="store_true",
        help="Enable application profiling, written to log directory"
    )
    return parser.parse_args()


def run_cli():
    """Runs the command-line interface, parses arguments, and initializes the logger and configuration."""
    args = parse_arguments()
    target_type = args.target_type
    config_format = args.config_format.lower()
    debug_mode = args.debug
    profiling = args.profiling

    # Convert config format to file extension
    if config_format in ["yaml", "json", "toml"]:
        config_format = "." + config_format
    else:
        print(f"Unsupported config format: {config_format}")
        return None, None, None

    config_file = Path(f"files/configs/{target_type}{config_format}")
    try:
        cfg = load_config(config_file)
    except Exception as e:
        print(f"Fatal Error (config.load_config): {e}")
        return None, None, None

    logger_cfg = cfg["logging"]

    if debug_mode:
        logger_cfg.log_level = "DEBUG"
    else:
        logger_cfg.log_level = "ERROR"

    logger = StructuredLogger(target_type, logger_cfg)

    if profiling:
        profile = RuntimeProfile(target_type, logger_cfg.log_directory)
        return logger, cfg, profile

    return logger, cfg, None
