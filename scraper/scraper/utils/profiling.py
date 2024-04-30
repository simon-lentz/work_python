import cProfile
import pstats
from pathlib import Path


class RuntimeProfile:
    """
    A class for profiling the runtime performance of the web scraper.

    Attributes:
        log_directory (Path): The directory where the profiling results will be stored.
        profiler (cProfile.Profile): The profiler instance.
        file_name (str): The name of the file where the profiling results will be saved.

    Methods:
        start(): Starts the profiling.
        stop(): Stops the profiling.
        write_stats(): Writes the profiling statistics to a file.
    """

    def __init__(self, target_type: str, log_directory: Path):
        """
        Initializes the RuntimeProfile with the target type and log directory.

        Args:
            target_type (str): The type of the target being profiled.
            log_directory (Path): The directory where the profiling results will be stored.
        """
        self.log_directory = log_directory
        self.profiler = cProfile.Profile()
        self.file_name = f"{target_type}_runtime.prof"

    def start(self):
        """Starts the profiling."""
        self.profiler.enable()

    def stop(self):
        """Stops the profiling."""
        self.profiler.disable()

    def write_stats(self):
        """
        Writes the profiling statistics to a file in the specified log directory.

        The statistics are sorted by cumulative time and written to a file with the
        name specified in the `file_name` attribute.
        """
        log_file = self.log_directory / self.file_name
        with log_file.open('w') as f:
            stats = pstats.Stats(self.profiler, stream=f).sort_stats('cumulative')
            stats.print_stats()
