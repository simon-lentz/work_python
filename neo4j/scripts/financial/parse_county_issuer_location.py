import os
import re
import logging
import pandas as pd
from typing import List
from pathlib import Path

# Set chained assigned opt for pd
pd.set_option('mode.chained_assignment', None)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
FINANCIAL_DATA_DIR = Path.cwd() / "neo4j" / "financial" / "data"
PLACE_DATA_DIR = Path.cwd() / "neo4j" / "place" / "data"
STATE_LOCATORS = (("AL", "01"), ("AR", "05"), ("AZ", "04"))


# Function to parse dates in the format 'MM/DD/YYYY'
def parse_dates(date):
    return pd.to_datetime(date, format='%m/%d/%Y')


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


def load_issuer_data(filepath: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath,
                         dtype={
                             "Issue Description": str,
                             "Issue Homepage": str,
                             "MSRB Issue Identifier": str,
                             "Maturity Date": str,
                             "Official Statement": str,
                             "Issuer Name": str,
                             "MSRB Issuer Identifier": str,
                             "State Abbreviation": str,
                             "State FIPS": str,
                             "Issuer Homepage": str,
                             "Issuer Type": str
                         },
                         parse_dates=["Dated Date", "Date Retrieved"],
                         date_format=parse_dates
                         )
        logging.info(f"Data loaded successfully from {filepath}.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")
        raise


def prepare_county_place_data(state_abbr: str) -> tuple:
    # Load place data
    place_data = PLACE_DATA_DIR / state_abbr / "place_data.csv"
    place_df = load_place_data(place_data)
    original_place_df = place_df.copy()
    place_df = place_df[['County Name', 'County FIPS']]
    matching_set = []
    for _, entry in place_df.iterrows():
        name = entry['County Name']
        fips = entry['County FIPS']
        match_tuple = (name, fips)
        matching_set.append(match_tuple)

    return original_place_df, matching_set


def prepare_county_issuer_data(state_abbr: str) -> tuple:
    # Load merged county issuers file
    county_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_county_issuers.csv"
    county_issuers_df = load_issuer_data(county_issuers_file)
    county_issuers_original_df = county_issuers_df.copy()

    # Filter and preprocess
    county_issuers_df = county_issuers_df[['MSRB Issuer Identifier', 'Issuer Name', 'Issue Description']].fillna('')

    # Function to replace non-alphanumeric characters with spaces
    def clean_text(text):
        # Replace non-alphanumeric characters with spaces
        return re.sub(r'[^a-zA-Z0-9\s]', ' ', text).strip().lower()

    county_issuers_df['Issuer Name'] = county_issuers_df['Issuer Name'].apply(clean_text)
    county_issuers_df['Issue Description'] = county_issuers_df['Issue Description'].apply(clean_text)

    return county_issuers_original_df, county_issuers_df


def match_issuers(matching_set: List[tuple], county_issuers_df: pd.DataFrame) -> pd.DataFrame:
    # Initialize an empty DataFrame for matched issuers
    matched_columns = ['MSRB Issuer Identifier', 'County FIPS']
    matched_df = pd.DataFrame(columns=matched_columns)

    # Iterate over each issuer and match based on the place names in the matching map
    for _, issuer in county_issuers_df.iterrows():
        issuer_name = issuer['Issuer Name']
        issue_description = issuer['Issue Description']
        for county_name, county_fips in matching_set:
            # Check if county name is a substring of the issuer name or issue description
            if county_name in issuer_name or county_name in issue_description:
                match = pd.DataFrame({
                    'MSRB Issuer Identifier': [issuer['MSRB Issuer Identifier']],
                    'County FIPS': [county_fips]
                })
                matched_df = pd.concat([matched_df, match], ignore_index=True)
                # If only the first match is needed, uncomment the next line:
                # break

    return matched_df


def merge_dfs(matched: pd.DataFrame, place: pd.DataFrame, issuers: pd.DataFrame) -> pd.DataFrame:
    # Merge the matched df on "CBSA Code" with the original place df
    merged_with_place = pd.merge(matched, place, on="County FIPS", how="left")

    # Then merge again on "MSRB Issuer Identifier" with original county issuer df
    final_merged_df = pd.merge(merged_with_place, issuers, on="MSRB Issuer Identifier", how="left")

    # Rename 'State FIPS_x' to 'State FIPS' if it exists
    if 'State FIPS_x' in final_merged_df.columns:
        final_merged_df.rename(columns={'State FIPS_x': 'State FIPS'}, inplace=True)

    # Reorder and select specific columns for the final output
    desired_columns = [
        'Issuer Name', 'Issuer Type', 'Issuer Homepage', 'MSRB Issuer Identifier',
        'County Name', 'County FIPS', 'State Abbreviation', 'State FIPS', 'Date Retrieved'
    ]

    # Ensure all desired columns are present (fill missing with empty strings or NaNs as placeholder)
    final_merged_df = final_merged_df.reindex(columns=desired_columns)

    # Drop duplicate issuer ids
    final_merged_df = final_merged_df.drop_duplicates(subset="MSRB Issuer Identifier")

    return final_merged_df


def parse_county_issuers(state_abbr: str) -> None:
    # Load and filter input data
    original_county_issuer_df, county_issuers_df = prepare_county_issuer_data(state_abbr)
    original_place_df, matching_map = prepare_county_place_data(state_abbr)

    # Use matching map and filtered issuers df to create a matched county issuers df
    matched_county_issuers_df = match_issuers(matching_map, county_issuers_df)

    # Merge matched and original data
    merged_df = merge_dfs(matched_county_issuers_df, original_place_df, original_county_issuer_df)

    # Output final merged DataFrame to CSV
    output_file = FINANCIAL_DATA_DIR / state_abbr / "county_issuers.csv"
    merged_df.to_csv(output_file, index=False)
    logging.info(f"Matched county issuers data written to {output_file}")

    # Find difference of set of matched issuer ids and original input issuer ids
    matched_ids = set(matched_county_issuers_df['MSRB Issuer Identifier'])
    original_ids = set(original_county_issuer_df['MSRB Issuer Identifier'])
    unmatched_ids = original_ids.difference(matched_ids)

    unmatched_issuers_df = original_county_issuer_df[original_county_issuer_df['MSRB Issuer Identifier'].isin(unmatched_ids)]  # noqa:E501

    unmatched_output_file = FINANCIAL_DATA_DIR / state_abbr / "unmatched_county_issuers.csv"
    unmatched_issuers_df.to_csv(unmatched_output_file, index=False)
    logging.info(f"Unmatched county issuers data written to {unmatched_output_file}")


def parse_county_issuer_location(state_abbr: str):
    # construct file paths
    county_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_county_issuers.csv"

    # if county issuers exist for state, parse them
    if os.path.exists(county_merged_issuers_file):
        parse_county_issuers(state_abbr)
    else:
        logging.info(f"No county issuers found for {state_abbr}")


def parse_by_state():
    for state_abbr, _ in STATE_LOCATORS:
        parse_county_issuer_location(state_abbr)


parse_by_state()
