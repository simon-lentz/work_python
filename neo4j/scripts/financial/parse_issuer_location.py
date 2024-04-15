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


def match_city_issuers(place_df: pd.DataFrame, city_issuers_df: pd.DataFrame) -> tuple:
    # Preprocessing
    city_issuers_df = city_issuers_df[["Issue Description", "Issuer Name", "MSRB Issuer Identifier"]].copy()
    # Ensure no NaN values and apply lower case and stripping
    city_issuers_df["Issue Description"] = city_issuers_df["Issue Description"].fillna('').apply(lambda x: x.strip().lower())  # noqa:E501
    city_issuers_df["Issuer Name"] = city_issuers_df["Issuer Name"].fillna('').apply(lambda x: x.strip().lower())
    place_df = place_df[["CBSA Code", "CBSA Title"]].dropna().drop_duplicates("CBSA Code").copy()
    place_df['CBSA Title'] = place_df['CBSA Title'].apply(lambda x: re.sub(r',.*', '', x).strip().lower())
    # Initialize matched DataFrame
    matched_df = pd.DataFrame(columns=['MSRB Issuer Identifier', 'CBSA Code'])
    # Match loop
    for _, place_row in place_df.iterrows():
        cbsa_title = place_row['CBSA Title']
        mask = city_issuers_df['Issuer Name'].str.contains(f"\\b{cbsa_title}\\b", na=False)
        temp_df = city_issuers_df[mask].copy()
        temp_df['CBSA Code'] = place_row['CBSA Code']
        matched_df = pd.concat([matched_df, temp_df[['MSRB Issuer Identifier', 'CBSA Code']]], ignore_index=True)
    return matched_df


def parse_issuer_location(state_abbr: str):
    state_dir = FINANCIAL_DATA_DIR / state_abbr
    place_data_file = state_dir / "place_data.csv"
    city_merged_issuers_file = os.path.join(state_dir, "city_merged_issuers.csv")
    county_merged_issuers_file = os.path.join(state_dir, "county_merged_issuers.csv")
    other_merged_issuers_file = os.path.join(state_dir, "other_merged_issuers.csv")
    if os.path.exists(city_merged_issuers_file):
        city_issuers_df = load_data(city_merged_issuers_file)
        place_df = load_data(place_data_file)
        # Perform the matching operation
        matched_df = match_city_issuers(place_df, city_issuers_df)
        # filter city_issuers_df before merge
        filtered_issuers_df = city_issuers_df[["Date Retrieved", "Issuer Name", "MSRB Issuer Identifier", "State Abbreviation", "State FIPS", "Issuer Homepage", "Issuer Type"]].copy()  # noqa:E501
        # Ensure unique MSRB Issuer Identifier in filtered_issuers_df
        filtered_issuers_df = filtered_issuers_df.drop_duplicates(subset=['MSRB Issuer Identifier'])
        # Merge matched_df and filtered_issuers_df on "MSRB Issuer Identifier"
        output_df = pd.merge(matched_df, filtered_issuers_df, on="MSRB Issuer Identifier", how='left')
        # Save the final matched DataFrame to CSV
        output_df.to_csv(os.path.join(state_dir, "detailed_matched.csv"), index=False)
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
