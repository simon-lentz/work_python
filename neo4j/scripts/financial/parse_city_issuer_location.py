import os
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


# Function to parse dates in the format 'DD/MM/YYYY'
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


def prepare_city_place_data(state_abbr: str) -> tuple:
    # Load place data
    place_data = PLACE_DATA_DIR / state_abbr / "place_data.csv"
    place_df = load_place_data(place_data)
    original_place_df = place_df.copy()
    place_df = place_df.dropna(subset=["CBSA Code"])
    place_df = place_df[['CBSA Code', 'CBSA Title', 'Metropolitan Division Title', 'CSA Title']].fillna('')

    # Clean titles
    def clean_title(title):
        first_part = title.split(',')[0].strip().lower()
        cleaned_title = first_part.replace('-', ',')
        return cleaned_title

    place_df['CBSA Title'] = place_df['CBSA Title'].apply(clean_title)
    place_df['Metropolitan Division Title'] = place_df['Metropolitan Division Title'].apply(clean_title)
    place_df['CSA Title'] = place_df['CSA Title'].apply(clean_title)

    # Create the matching map
    matching_map = {}
    for index, row in place_df.iterrows():
        places = set()
        places.update(row['CBSA Title'].split(',') + row['Metropolitan Division Title'].split(',') + row['CSA Title'].split(','))  # noqa:E501
        places.discard('')
        if places:
            matching_map[row['CBSA Code']] = list(places)

    return original_place_df, matching_map


def prepare_city_issuer_data(state_abbr: str) -> tuple:
    # Load merged city issuers file
    city_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_city_issuers.csv"
    city_issuers_df = load_issuer_data(city_issuers_file)
    city_issuers_original_df = city_issuers_df.copy()

    # Filter and preprocess
    city_issuers_df = city_issuers_df[['MSRB Issuer Identifier', 'Issuer Name']].fillna('')
    city_issuers_df['Issuer Name'] = city_issuers_df['Issuer Name'].apply(lambda x: x.strip().lower())

    return city_issuers_original_df, city_issuers_df


def match_city_issuers(matching_map: dict, city_issuers_df: pd.DataFrame) -> pd.DataFrame:
    # Initialize an empty DataFrame for matched issuers
    matched_columns = ['MSRB Issuer Identifier', 'CBSA Code']
    matched_df = pd.DataFrame(columns=matched_columns)

    # Iterate over each issuer and match based on the place names in the matching map
    for index, issuer in city_issuers_df.iterrows():
        issuer_name = issuer['Issuer Name']
        for cbsa_code, places in matching_map.items():
            # Check if any place name from the matching map is a substring of the issuer name
            if any(place in issuer_name for place in places):
                match = pd.DataFrame({
                    'MSRB Issuer Identifier': [issuer['MSRB Issuer Identifier']],
                    'CBSA Code': [cbsa_code]
                })
                matched_df = pd.concat([matched_df, match], ignore_index=True)

    return matched_df


def parse_city_issuers(state_abbr: str) -> None:
    # Load and filter input data
    original_city_issuer_df, city_issuers_df = prepare_city_issuer_data(state_abbr)
    original_place_df, matching_map = prepare_city_place_data(state_abbr)

    # Use matching map and filtered issuers df to create a matched city issuers df
    matched_city_issuers_df = match_city_issuers(matching_map, city_issuers_df)

    # Merge the matched df on "CBSA Code" with the original place df
    merged_with_place = pd.merge(matched_city_issuers_df, original_place_df, on="CBSA Code", how="left")

    # Then merge again on "MSRB Issuer Identifier" with original city issuer df
    final_merged_df = pd.merge(merged_with_place, original_city_issuer_df, on="MSRB Issuer Identifier", how="left")

    # Rename 'State FIPS_x' to 'State FIPS' if it exists
    if 'State FIPS_x' in final_merged_df.columns:
        final_merged_df.rename(columns={'State FIPS_x': 'State FIPS'}, inplace=True)

    # Reorder and select specific columns for the final output
    desired_columns = [
        'Issuer Name', 'Issuer Type', 'Issuer Homepage', 'MSRB Issuer Identifier',
        'CBSA Code', 'CBSA Title', 'Metropolitan Division Title', 'CSA Title',
        'County Name', 'County FIPS', 'State Abbreviation', 'State FIPS', 'Date Retrieved'
    ]

    # Ensure all desired columns are present (fill missing with empty strings or NaNs as placeholder)
    final_merged_df = final_merged_df.reindex(columns=desired_columns)

    # Drop duplicate issuer ids
    final_merged_df = final_merged_df.drop_duplicates(subset="MSRB Issuer Identifier")

    # Output final merged DataFrame to CSV
    output_file = FINANCIAL_DATA_DIR / state_abbr / "city_issuers.csv"
    final_merged_df.to_csv(output_file, index=False)
    logging.info(f"Final matched city issuers data written to {output_file}")

    # Find difference of set of matched issuer ids and original input issuer ids
    matched_ids = set(matched_city_issuers_df['MSRB Issuer Identifier'])
    original_ids = set(original_city_issuer_df['MSRB Issuer Identifier'])
    unmatched_ids = original_ids - matched_ids

    # Filter original with this difference to create a df of unmatched issuers
    unmatched_issuers_df = original_city_issuer_df[original_city_issuer_df['MSRB Issuer Identifier'].isin(unmatched_ids)]  # noqa:E501

    # Output as unmatched_city_issuers.csv
    unmatched_output_file = FINANCIAL_DATA_DIR / state_abbr / "unmatched_city_issuers.csv"
    unmatched_issuers_df.to_csv(unmatched_output_file, index=False)
    logging.info(f"Unmatched city issuers data written to {unmatched_output_file}")


def parse_city_issuer_location(state_abbr: str):
    # construct file paths
    city_merged_issuers_file = FINANCIAL_DATA_DIR / state_abbr / "merged_city_issuers.csv"

    # if city issuers exist for state, parse them
    if os.path.exists(city_merged_issuers_file):
        parse_city_issuers(state_abbr)
    else:
        logging.info(f"No City issuers found for {state_abbr}")


def parse_by_state():
    for state_abbr, _ in STATE_LOCATORS:
        parse_city_issuer_location(state_abbr)


parse_by_state()
