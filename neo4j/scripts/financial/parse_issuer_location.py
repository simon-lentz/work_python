import os
import re
import logging
import pandas as pd
from pathlib import Path

# Set chained assigned opt for pd
pd.set_option('mode.chained_assignment', None)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
FINANCIAL_DATA_DIR = Path.cwd() / "neo4j" / "financial" / "data"
PLACE_DATA_DIR = Path.cwd() / "neo4j" / "place" / "data"
STATE_LOCATORS = (("AK", "02"), ("AL", "01"))
ISSUER_TYPES = ("City", "County", "Other")


def load_data(filepath: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath, dtype=str)
        logging.info(f"Data loaded successfully from {filepath}.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")
        raise


def prepare_place_data(state_abbr: str) -> pd.DataFrame:
    place_data = PLACE_DATA_DIR / state_abbr / "place_data.csv"
    place_df = load_data(place_data)
    place_df = place_df[["CBSA Code", "CBSA Title"]].dropna().drop_duplicates("CBSA Code").copy()
    place_df['CBSA Title'] = place_df['CBSA Title'].apply(lambda x: re.sub(r',.*', '', x).strip().lower())
    return place_df


def prepare_city_issuer_data(state_abbr: str) -> pd.DataFrame:
    # Load merged_city_issuers file
    city_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_city_issuers.csv"
    city_issuers_df = load_data(city_issuers_file)

    # Debug: print available columns
    logging.debug(f"Available columns in city_issuers_df: {city_issuers_df.columns.tolist()}")

    # Preprocess: Ensure no NaN values and apply lower case and stripping
    city_issuers_df["Issue Description"] = city_issuers_df.get("Issue Description", "").fillna('').apply(lambda x: x.strip().lower())  # noqa:E501
    city_issuers_df["Issuer Name"] = city_issuers_df.get("Issuer Name", "").fillna('').apply(lambda x: x.strip().lower())  # noqa:E501

    return city_issuers_df


def match_city_issuers(place_df: pd.DataFrame, city_issuers_df: pd.DataFrame) -> tuple:
    # Initialize matched DataFrame
    matched_df = pd.DataFrame(columns=['MSRB Issuer Identifier', 'CBSA Code'])
    # attempt direct matching
    for _, place_row in place_df.iterrows():
        cbsa_title = place_row['CBSA Title']
        mask = city_issuers_df['Issuer Name'].str.contains(f"\\b{cbsa_title}\\b", na=False)
        temp_df = city_issuers_df[mask].copy()
        temp_df['CBSA Code'] = place_row['CBSA Code']
        matched_df = pd.concat([matched_df, temp_df[['MSRB Issuer Identifier', 'CBSA Code']]], ignore_index=True)
    return matched_df


def parse_city_issuers(state_abbr: str) -> None:
    city_issuers_df = prepare_city_issuer_data(state_abbr)
    place_df = prepare_place_data(state_abbr)
    matched_df = match_city_issuers(place_df, city_issuers_df)

    # Ensure the DataFrame has expected columns before filtering
    expected_columns = ["Date Retrieved", "Issuer Name", "MSRB Issuer Identifier", "State Abbreviation", "State FIPS", "Issuer Homepage", "Issuer Type"]  # noqa:E501
    missing_columns = [col for col in expected_columns if col not in city_issuers_df.columns]
    if missing_columns:
        logging.error(f"Missing columns in city_issuers_df: {missing_columns}")
        return  # Exit the function if required columns are missing

    filtered_issuers_df = city_issuers_df[expected_columns].drop_duplicates(subset=['MSRB Issuer Identifier'])
    output_df = pd.merge(matched_df, filtered_issuers_df, on="MSRB Issuer Identifier", how='left')
    output_file = FINANCIAL_DATA_DIR / state_abbr / "city_issuers.csv"
    output_df.to_csv(output_file, index=False)


def parse_issuer_location(state_abbr: str):
    # construct file paths
    city_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_city_issuers.csv"
    county_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_county_issuers.csv"
    other_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_other_issuers.csv"

    if os.path.exists(city_merged_issuers_file):
        parse_city_issuers(state_abbr)
    if os.path.exists(county_merged_issuers_file):
        # Implement similar logic for county issuers if necessary
        pass
    if os.path.exists(other_merged_issuers_file):
        # Implement similar logic for other issuers if necessary
        pass


def parse_by_state():
    for state_abbr, _ in STATE_LOCATORS:
        parse_issuer_location(state_abbr)


parse_by_state()
