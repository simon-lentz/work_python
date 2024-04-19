import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
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


def load_county_data(filepath: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath,
                         dtype={
                             "State FIPS": str,
                             "State Name": str,
                             "County FIPS": str,
                             "County Name": str
                         })
        logging.info(f"Data loaded successfully from {filepath}.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")
        raise


def load_city_data(filepath: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath,
                         dtype={
                             "CBSA Code": str,
                             "Metropolitan Division Code": str,
                             "CSA Code": str,
                             "CBSA Title": str,
                             "Metropolitan/Micropolitan Statistical Area": str,
                             "Metropolitan Division Title": str,
                             "CSA Title": str,
                             "County/County Equivalent": str,
                             "State Name": str,
                             "State FIPS": str,
                             "County FIPS": str,
                             "Central/Outlying County": str
                         })
        logging.info(f"Data loaded successfully from {filepath}.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")


def filter_and_merge_data(counties_df: pd.DataFrame, cities_df: pd.DataFrame, state_fips_code: str) -> pd.DataFrame:
    try:
        filtered_counties = counties_df[counties_df["State FIPS"] == state_fips_code]
        filtered_cities = cities_df[cities_df["State FIPS"] == state_fips_code]
        if filtered_counties.empty or filtered_cities.empty:
            logging.error("Filtered data is empty. Check state FIPS code and data integrity.")
        # Perform the merge
        merged_df = pd.merge(filtered_counties, cities_df, on='County FIPS', how='left', suffixes=('_x', '_y'))
        # Drop columns that end with '_y'
        merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_y')]]
        # Remove '_x' from any columns that have it
        merged_df.columns = [col.replace('_x', '') for col in merged_df.columns]
        # Define columns to drop
        columns_to_drop = ['CSA Code', 'Metropolitan/Micropolitan Statistical Area', 'County/County Equivalent', 'State Name', 'Central/Outlying County', 'Metropolitan Division Code']  # noqa:E501
        # Drop specified columns
        merged_df = merged_df.drop(columns=columns_to_drop, errors='ignore')
        return merged_df
    except KeyError as ke:
        logging.error(f"Missing key for filtering or merging: {ke}")


def process_state_data(state_abbr: str, state_fips_code: str):
    logging.info(f"Processing data for {state_abbr}...")
    counties_path = PLACE_DATA_DIR / "counties.csv"
    cities_path = PLACE_DATA_DIR / "cities.csv"
    counties_df = load_county_data(counties_path)
    cities_df = load_city_data(cities_path)
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
            continue


merge_by_state()
