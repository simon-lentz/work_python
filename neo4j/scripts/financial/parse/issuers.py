import logging
import pandas as pd
from pathlib import Path
from .utils import setup_logging, parse_dates
from .city_issuers import load_city_issuer_data, city_location_map, match_city_issuer, merge_city_issuers
from .county_issuers import load_county_issuer_data, county_location_map, match_county_issuer, merge_county_issuers


# Initialize logger
setup_logging()

# Set chained assigned opt for pd
pd.set_option('mode.chained_assignment', None)

# Constants
FINANCIAL_DATA_DIR = Path.cwd() / "neo4j" / "financial" / "data"
PLACE_DATA_DIR = Path.cwd() / "neo4j" / "place" / "data"
STATE_LOCATORS = (("AK", "02"), ("AL", "01"))


# Function to load input state issuer data
def process_state_and_other_issuer_data(input_filepath: Path, output_filepath: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(input_filepath,
                         dtype={
                             "Issuer Name": str,
                             "Issuer Homepage": str,
                             "MSRB Issuer Identifier": str,
                             "Issuer Type": str,
                             "State Abbreviation": str,
                             "State FIPS": str
                         },
                         parse_dates=["Date Retrieved"],
                         date_format=parse_dates
                         )
        logging.info(f"Data loaded successfully from {input_filepath}.")

        # Filter df where issuer type is "State"
        state_df = df.copy()
        state_issuer_df = state_df[state_df['Issuer Type'] == 'State']
        state_output = output_filepath / "state_issuers.csv"
        state_issuer_df.to_csv(state_output, index=False)

        # Filter original_df where issuer type is "Other"
        other_df = df.copy()
        other_issuer_df = other_df[other_df['Issuer Type'] == 'Other']
        other_output = output_filepath / "other_issuers.csv"
        other_issuer_df.to_csv(other_output, index=False)

        # Return original df
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {input_filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {input_filepath}.")
        raise


# Function to load input place data
def load_place_data(filepath: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath,
                         dtype={
                             "State FIPS": str,
                             "County FIPS": str,
                             "County Name": str,
                             "CBSA Code": str,
                             "CBSA Title": str,
                             "Metropolitan Division Title": str,
                             "CSA Title": str
                         }
                         )
        logging.info(f"Data loaded successfully from {filepath}.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")
        raise


def parse_issuers(state_abbr: str) -> None:
    # Initialize unmatched issuers list
    unmatched_issuers = []

    # Load place data
    place_data_path = PLACE_DATA_DIR / state_abbr / "place_data.csv"
    place_df = load_place_data(place_data_path)

    # Create mappings with place data
    city_mapping = city_location_map(place_df)
    county_mapping = county_location_map(place_df)

    # Filter out state and other issuers
    issuer_data_path = FINANCIAL_DATA_DIR / state_abbr / "issuers.csv"
    output_path = FINANCIAL_DATA_DIR / state_abbr
    original_issuers_df = process_state_and_other_issuer_data(issuer_data_path, output_path)

    # Match, merge, and write city issuers
    original_city_issuers_df = load_city_issuer_data(issuer_data_path)
    matched_city_issuers_df = match_city_issuer(issuer_data_path, city_mapping)
    if not matched_city_issuers_df.empty:
        city_issuers_df, unmatched_city_issuers = merge_city_issuers(original_city_issuers_df, place_df, matched_city_issuers_df)  # noqa:E501
        unmatched_issuers.extend(unmatched_city_issuers)
        city_output_path = output_path / "city_issuers.csv"
        city_issuers_df.to_csv(city_output_path, index=False)
    else:
        logging.info(f"No matches found for city issuers in {state_abbr}.")

    # Match, merge and write county issuers
    original_county_issuers_df = load_county_issuer_data(issuer_data_path)
    matched_county_issuers_df = match_county_issuer(issuer_data_path, county_mapping)
    if not matched_county_issuers_df.empty:
        county_issuers_df, unmatched_county_issuers = merge_county_issuers(original_county_issuers_df, place_df, matched_county_issuers_df)  # noqa:E501
        unmatched_issuers.extend(unmatched_county_issuers)
        county_output_path = output_path / "county_issuers.csv"
        county_issuers_df.to_csv(county_output_path, index=False)
    else:
        logging.info(f"No matches found for county issuers in {state_abbr}.")

    # Handle unmatched issuers
    if len(unmatched_issuers) > 0:
        # Filter original_issuers_df to find unmatched issuer details
        unmatched_issuers_df = original_issuers_df[original_issuers_df['MSRB Issuer Identifier'].isin(unmatched_issuers)]  # noqa:E501
        unmatched_output_path = output_path / "unmatched_issuers.csv"
        unmatched_issuers_df.to_csv(unmatched_output_path, index=False)
        logging.info(f"Unmatched issuers written to {unmatched_output_path}")
    else:
        logging.info("No unmatched issuers found.")


def parse_by_state():
    for state_abbr, _ in STATE_LOCATORS:
        parse_issuers(state_abbr)
