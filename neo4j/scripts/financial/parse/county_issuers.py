import re
import logging
import pandas as pd
from pathlib import Path
from .utils import parse_dates, setup_logging


setup_logging()

# Set chained assigned opt for pd
pd.set_option('mode.chained_assignment', None)


# Function to load input county issuer data
def load_county_issuer_data(filepath: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath,
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
        logging.info(f"Data loaded successfully from {filepath}.")

        # Filter df where issuer type is "County"
        city_issuer_df = df[df['Issuer Type'] == 'County']
        return city_issuer_df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")
        raise


# Function to create mapping from county name to county issuer name
def county_location_map(place_df: pd.DataFrame) -> list:
    county_map_df = place_df.copy()
    county_map_df = county_map_df[['County Name', 'County FIPS']]

    def clean_county_text(text):
        return re.sub(r'[^a-zA-Z0-9\s]', ' ', text).strip().lower()

    county_map = []
    for _, entry in county_map_df.iterrows():
        full_name = clean_county_text(entry['County Name'].strip().lower())
        abbreviated_name = ' '.join(part for part in full_name.split() if part != 'county').strip()
        fips = entry['County FIPS']
        match_tuple = (full_name, clean_county_text(abbreviated_name), fips)
        county_map.append(match_tuple)

    return county_map


def match_county_issuer(issuers_path: Path, mapping: list) -> pd.DataFrame:
    county_issuers_df = load_county_issuer_data(issuers_path)

    matches = []

    for _, issuer in county_issuers_df.iterrows():
        issuer_name_lower = issuer['Issuer Name'].lower()  # Convert to lower case once
        for full_name, abbreviated_name, county_fips in mapping:
            # Check if either the full name or abbreviated name matches the issuer name
            if any(name in issuer_name_lower for name in (full_name, abbreviated_name)):
                matches.append({
                    'MSRB Issuer Identifier': issuer['MSRB Issuer Identifier'],
                    'County FIPS': county_fips
                })
                break  # Move to the next issuer after a successful match

    matched_df = pd.DataFrame(matches)
    return matched_df


def merge_county_issuers(original_df: pd.DataFrame, place_df: pd.DataFrame, matched_df: pd.DataFrame) -> tuple:
    # First merge: Combine matched_df with original_df on MSRB Issuer Identifier
    merged_data = pd.merge(matched_df, original_df, on='MSRB Issuer Identifier', how='left')

    # Second merge: Enrich the above result by merging with place_df on County FIPS
    final_merged_data = pd.merge(merged_data, place_df, on='County FIPS', how='left')

    # Drop 'State FIPS_y' column, rename 'State FIPS_x' column to 'State FIPS'
    final_merged_data = final_merged_data.drop(columns=['State FIPS_y'])
    final_merged_data = final_merged_data.rename(columns={'State FIPS_x': 'State FIPS'})

    # Filter out any duplicate MSRB Issuer Identifier from final_merged_data
    final_merged_data = final_merged_data.drop_duplicates(subset=['MSRB Issuer Identifier'])

    # Use original and matched df to find unmatched issuers
    input_county_issuers = set(original_df['MSRB Issuer Identifier'])
    matched_county_issuers = set(final_merged_data['MSRB Issuer Identifier'])
    unmatched_county_issuers = input_county_issuers - matched_county_issuers

    return final_merged_data, unmatched_county_issuers
