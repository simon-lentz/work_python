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
STATE_LOCATORS = (("AK", "02"), ("MS", "28"), ("AL", "01"), ("MT", "30"), ("AR", "05"),
                  ("NC", "37"), ("ND", "38"), ("AZ", "04"), ("NE", "31"), ("CA", "06"),
                  ("NH", "33"), ("CO", "08"), ("NJ", "34"), ("CT", "09"), ("NM", "35"),
                  ("DC", "11"), ("NV", "32"), ("DE", "10"), ("NY", "36"), ("FL", "12"),
                  ("OH", "39"), ("GA", "13"), ("OK", "40"), ("OR", "41"), ("HI", "15"),
                  ("PA", "42"), ("IA", "19"), ("ID", "16"), ("RI", "44"), ("IL", "17"),
                  ("SC", "45"), ("IN", "18"), ("SD", "46"), ("KS", "20"), ("TN", "47"),
                  ("KY", "21"), ("TX", "48"), ("LA", "22"), ("UT", "49"), ("MA", "25"),
                  ("VA", "51"), ("MD", "24"), ("ME", "23"), ("VT", "50"), ("MI", "26"),
                  ("WA", "53"), ("MN", "27"), ("WI", "55"), ("MO", "29"), ("WV", "54"),
                  ("WY", "56"))


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

    # Function to replace non-alphanumeric characters with spaces
    def clean_text(text):
        # Replace non-alphanumeric characters with spaces
        return re.sub(r'[^a-zA-Z0-9\s]', ' ', text).strip().lower()

    for _, entry in place_df.iterrows():
        full_name = clean_text(entry['County Name'].strip().lower())
        # Remove "county" and any other common suffixes and strip again
        abbreviated_name = ' '.join(part for part in full_name.split() if part != 'county').strip()
        fips = entry['County FIPS']
        match_tuple = (full_name, clean_text(abbreviated_name), fips)
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
    matches = []

    for _, issuer in county_issuers_df.iterrows():
        issuer_name = issuer['Issuer Name']
        issue_description = issuer['Issue Description']
        for full_name, abbreviated_name, county_fips in matching_set:
            # First try to match using the full county name in either issuer name or issue description
            if full_name in issuer_name or full_name in issue_description:
                matches.append({
                    'MSRB Issuer Identifier': issuer['MSRB Issuer Identifier'],
                    'County FIPS': county_fips
                })
                break  # Move to the next issuer after a successful match
            # If no match found with full name, check using the abbreviated name
            elif abbreviated_name in issuer_name or abbreviated_name in issue_description:
                matches.append({
                    'MSRB Issuer Identifier': issuer['MSRB Issuer Identifier'],
                    'County FIPS': county_fips
                })
                break  # Move to the next issuer after a successful match
    matched_df = pd.DataFrame(matches)
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
    original_place_df, matching_set = prepare_county_place_data(state_abbr)

    # Use matching map and filtered issuers df to create a matched county issuers df
    matched_county_issuers_df = match_issuers(matching_set, county_issuers_df)

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
    county_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "county_issuers.csv"
    # if county issuers have already been parsed, skip
    if os.path.exists(county_issuers_file):
        logging.info(f"County Issuers already processed for {state_abbr}")
        return
    # if county issuers exist for state, parse them
    if os.path.exists(county_merged_issuers_file):
        parse_county_issuers(state_abbr)
    else:
        logging.info(f"No county issuers found for {state_abbr}")


def parse_by_state():
    for state_abbr, _ in STATE_LOCATORS:
        parse_county_issuer_location(state_abbr)


parse_by_state()
