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
ISSUER_TYPES = ("City", "County", "Other")
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


def prepare_city_place_data(state_abbr: str) -> pd.DataFrame:
    place_data = PLACE_DATA_DIR / state_abbr / "place_data.csv"
    place_df = load_data(place_data)
    place_df = place_df[["CBSA Code", "CBSA Title"]].dropna().drop_duplicates("CBSA Code").copy()
    place_df['CBSA Title'] = place_df['CBSA Title'].apply(lambda x: re.sub(r',.*', '', x).strip().lower())
    return place_df


def prepare_city_issuer_data(state_abbr: str) -> tuple:
    # Load merged_city_issuers file
    city_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_city_issuers.csv"
    city_issuers_df = load_data(city_issuers_file)
    city_issuers_original_df = city_issuers_df.copy()

    # Debug: print available columns
    logging.debug(f"Available columns in city_issuers_df: {city_issuers_df.columns.tolist()}")

    # Preprocess: Ensure no NaN values and apply lower case and stripping
    city_issuers_df["Issuer Name"] = city_issuers_df.get("Issuer Name", "").fillna('').apply(lambda x: x.strip().lower())  # noqa:E501

    return city_issuers_original_df, city_issuers_df


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


def parse_city_issuers(state_abbr: str) -> pd.DataFrame:
    city_original, city_issuers_df = prepare_city_issuer_data(state_abbr)
    place_df = prepare_city_place_data(state_abbr)
    matched_df = match_city_issuers(place_df, city_issuers_df)
    # Ensure the DataFrame has expected columns before filtering
    expected_columns = ["Date Retrieved", "Issuer Name", "MSRB Issuer Identifier", "State Abbreviation", "State FIPS", "Issuer Homepage", "Issuer Type"]  # noqa:E501
    missing_columns = [col for col in expected_columns if col not in city_issuers_df.columns]
    if missing_columns:
        logging.error(f"Missing columns in city_issuers_df: {missing_columns}")
        return pd.DataFrame()  # Return an empty DataFrame to maintain return type consistency
    filtered_issuers_df = city_original[expected_columns].drop_duplicates(subset=['MSRB Issuer Identifier'])
    output_df = pd.merge(matched_df, filtered_issuers_df, on="MSRB Issuer Identifier", how='left')
    # Drop duplicates based on 'MSRB Issuer Identifier' to ensure each ID appears only once
    output_df = output_df.drop_duplicates(subset=['MSRB Issuer Identifier'])
    # Save the final matched DataFrame to CSV
    output_file = FINANCIAL_DATA_DIR / state_abbr / "city_issuers.csv"
    output_df.to_csv(output_file, index=False)
    # Find set difference between original and matched issuer ids
    unmatched_issuer_ids = set(filtered_issuers_df["MSRB Issuer Identifier"]) - set(matched_df["MSRB Issuer Identifier"])  # noqa:E501
    # Filter city_original to get the DataFrame of unmatched issuers
    unmatched_city_issuers_df = filtered_issuers_df[filtered_issuers_df["MSRB Issuer Identifier"].isin(unmatched_issuer_ids)]  # noqa:E501
    # Ensure that unmatched DataFrame also does not have duplicate MSRB Issuer Identifiers
    unmatched_city_issuers_df = unmatched_city_issuers_df.drop_duplicates(subset=['MSRB Issuer Identifier'])
    return unmatched_city_issuers_df


def parse_issuer_location(state_abbr: str):
    # construct file paths
    city_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_city_issuers.csv"
    county_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_county_issuers.csv"
    other_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_other_issuers.csv"

    if os.path.exists(city_merged_issuers_file):
        unmatched_df = parse_city_issuers(state_abbr)
        unmatched_output = FINANCIAL_DATA_DIR / state_abbr / "unmatched_city_issuers.csv"
        unmatched_df.to_csv(unmatched_output, index=False)
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
