from scraper.etl.target import TargetManager
from scraper.web.controller import WebController
from scraper.cmd.cli import run_cli


def main():
    """
    The entry point for the web scraper application.

    This function:
    - Parses command-line arguments and initializes logging and profiling.
    - Sets up connections for web scraping (including proxies, Docker containers, and web drivers).
    - Runs the web scraping process based on the provided configuration.
    - Handles exceptions and cleans up resources upon completion.
    """
    logger, cfg, profile = run_cli()

    if logger is None or cfg is None:
        return

    logger.info("Starting Web Scraper")

    if profile:
        profile.start()

    try:
        # Using the 'with' statement to manage the lifecycle of the WebController
        with WebController(logger, cfg) as controller:
            target_manager = TargetManager(logger, controller, cfg)
            target_manager.execute()

    except KeyboardInterrupt:
        logger.info("Interrupt received! Shutting down...")
    except Exception as e:
        logger.critical(f"Unrecoverable runtime exception raised: {e}", exc_info=True)
    finally:
        if profile:
            profile.stop()
            profile.write_stats()
        logger.info("Scraper Exited")
        logger.close()


if __name__ == "__main__":
    main()
