import logging
from pandas import to_datetime


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_dates(date_str, format='%m/%d/%Y'):
    return to_datetime(date_str, format=format)
