import logging
import pandas as pd
from pathlib import Path
from .utils import parse_dates, setup_logging


setup_logging()


# Set chained assigned opt for pd
pd.set_option('mode.chained_assignment', None)


# Function to load input city issuer data
def load_city_issuer_data(filepath: Path) -> pd.DataFrame:
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

        # Filter df where issuer type is "City"
        city_issuer_df = df[df['Issuer Type'] == 'City']
        return city_issuer_df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")
        raise


# Function to create the mappings from city place data to city issuer name
def city_location_map(place_df: pd.DataFrame) -> dict:
    city_map_df = place_df.copy()
    city_map_df = city_map_df.dropna(subset=["CBSA Code"])
    city_map_df = city_map_df[['CBSA Code', 'CBSA Title', 'Metropolitan Division Title', 'CSA Title']].fillna('')

    def clean_city_text(title):
        first_part = title.split(',')[0].strip().lower()
        cleaned_title = first_part.replace('-', ',')
        return cleaned_title

    city_map_df['CBSA Title'] = city_map_df['CBSA Title'].apply(clean_city_text)
    city_map_df['Metropolitan Division Title'] = city_map_df['Metropolitan Division Title'].apply(clean_city_text)
    city_map_df['CSA Title'] = city_map_df['CSA Title'].apply(clean_city_text)

    city_map = {}
    for _, row in city_map_df.iterrows():
        places = set()
        places.update(row['CBSA Title'].split(',') + row['Metropolitan Division Title'].split(',') + row['CSA Title'].split(','))  # noqa:E501
        places.discard('')
        if places:
            city_map[row['CBSA Code']] = list(places)

    return city_map


def match_city_issuer(issuers_path: Path, mapping: dict) -> pd.DataFrame:
    city_issuers_df = load_city_issuer_data(issuers_path)

    matches = []

    for _, issuer in city_issuers_df.iterrows():
        issuer_name = issuer['Issuer Name'].lower()  # Ensuring the comparison is case-insensitive
        for cbsa_code, places in mapping.items():
            # Iterate over each place name in the list of places for this CBSA code
            if any(place.lower() in issuer_name for place in places):
                # If any place matches the issuer name, append the match and break
                matches.append({
                    'MSRB Issuer Identifier': issuer['MSRB Issuer Identifier'],
                    'CBSA Code': cbsa_code
                })
                break  # Move to the next issuer after a successful match

    matched_df = pd.DataFrame(matches)
    return matched_df


def merge_city_issuers(original_df: pd.DataFrame, place_df: pd.DataFrame, matched_df: pd.DataFrame) -> tuple:
    # First merge: Combine matched_df with original_df on MSRB Issuer Identifier
    merged_data = pd.merge(matched_df, original_df, on='MSRB Issuer Identifier', how='left')

    # Second merge: Enrich the above result by merging with place_df on CBSA Code
    final_merged_data = pd.merge(merged_data, place_df, on='CBSA Code', how='left')

    # Drop 'State FIPS_y' column, rename 'State FIPS_x' column to 'State FIPS'
    final_merged_data = final_merged_data.drop(columns=['State FIPS_y'])
    final_merged_data = final_merged_data.rename(columns={'State FIPS_x': 'State FIPS'})

    # Filter out any duplicate MSRB Issuer Identifier from final_merged_data
    final_merged_data = final_merged_data.drop_duplicates(subset=['MSRB Issuer Identifier'])

    # Use original and matched df to find unmatched issuers
    input_city_issuers = set(original_df['MSRB Issuer Identifier'])
    matched_city_issuers = set(final_merged_data['MSRB Issuer Identifier'])
    unmatched_city_issuers = input_city_issuers - matched_city_issuers

    return final_merged_data, unmatched_city_issuers
