import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
PLACE_DATA_DIR = Path.cwd() / "neo4j" / "place" / "data"
STATE_LOCATORS = (("AK", "02"), ("AL", "01"))


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


def filter_and_merge_data(counties_df: pd.DataFrame, cities_df: pd.DataFrame, state_fips_code: str) -> pd.DataFrame:
    try:
        filtered_counties = counties_df[counties_df["State FIPS"] == state_fips_code]
        filtered_cities = cities_df[cities_df["State FIPS"] == state_fips_code]
        if filtered_counties.empty or filtered_cities.empty:
            raise ValueError("Filtered data is empty. Check state FIPS code and data integrity.")
        # Perform the merge
        merged_df = pd.merge(filtered_counties, cities_df, on='County FIPS', how='left', suffixes=('_x', '_y'))
        # Drop columns that end with '_y'
        merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_y')]]
        # Remove '_x' from any columns that have it
        merged_df.columns = [col.replace('_x', '') for col in merged_df.columns]
        # Define columns to drop
        columns_to_drop = ['Metropolitan Division Code', 'CSA Code', 'Metropolitan/Micropolitan Statistical Area', 'County/County Equivalent', 'State Name', 'Central/Outlying County']  # noqa:E501
        # Drop specified columns
        merged_df = merged_df.drop(columns=columns_to_drop, errors='ignore')
        return merged_df
    except KeyError as ke:
        logging.error(f"Missing key for filtering or merging: {ke}")
        raise


def process_state_data(state_abbr: str, state_fips_code: str):
    logging.info(f"Processing data for {state_abbr}...")
    counties_path = PLACE_DATA_DIR / "counties.csv"
    cities_path = PLACE_DATA_DIR / "cities.csv"
    counties_df = load_data(counties_path)
    cities_df = load_data(cities_path)
    merged_df = filter_and_merge_data(counties_df, cities_df, state_fips_code)
    output_path = PLACE_DATA_DIR / state_abbr / "place_data.csv"
    merged_df.to_csv(output_path, index=False)
    logging.info(f"Data for {state_abbr} processed and saved to {output_path}.")


def merge_by_state():
    for state_abbr, state_fips_code in STATE_LOCATORS:
        try:
            process_state_data(state_abbr, state_fips_code)
        except Exception as e:
            logging.error(f"Failed to process data for {state_abbr}: {e}")


merge_by_state()
