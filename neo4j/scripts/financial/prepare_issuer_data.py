import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
FINANCIAL_DATA_DIR = Path.cwd() / "neo4j" / "financial" / "data"
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


# Function to parse dates in the format 'DD/MM/YYYY'
def parse_dates(date):
    return pd.to_datetime(date, format='%d/%m/%Y')


def load_data(filepath: Path) -> pd.DataFrame:
    """
    Load data from a specified CSV file with all entries treated as strings.
    Raises FileNotFoundError if the file is not found, pd.errors.EmptyDataError if the file is empty.
    """
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


def load_issuer_data(filepath: Path) -> pd.DataFrame:
    """
    Load data from a specified CSV file with all entries treated as strings.
    Raises FileNotFoundError if the file is not found, pd.errors.EmptyDataError if the file is empty.
    """
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
                         parse_dates=['Date Retrieved'],
                         date_parser=parse_dates
                         )
        logging.info(f"Data loaded successfully from {filepath}.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {filepath}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {filepath}.")
        raise


def create_state_issuers(state_dir: Path, issuers_df: pd.DataFrame):
    state_issuers_df = issuers_df[issuers_df["Issuer Type"] == "State"]
    output_path = state_dir / "state_issuers.csv"
    state_issuers_df.to_csv(output_path, index=False)


def merge_financial_data(state_abbr: str) -> tuple:
    """
    Merges issues and issuers CSV data files based on 'MSRB Issuer Identifier'.
    Returns a tuple containing the path to save the data and the merged DataFrame.
    """
    try:
        issues_path = FINANCIAL_DATA_DIR / state_abbr / "issues.csv"
        issuers_path = FINANCIAL_DATA_DIR / state_abbr / "issuers.csv"
        issues_df = load_data(issues_path)
        issuers_df = load_issuer_data(issuers_path)
        merged_df = pd.merge(issues_df, issuers_df, on='MSRB Issuer Identifier', how='left')
        # Drop columns that end with '_y'
        merged_df = merged_df[[col for col in merged_df.columns if not col.endswith('_y')]]
        # Remove '_x' from any columns that have it
        merged_df.columns = [col.replace('_x', '') for col in merged_df.columns]
        state_dir = FINANCIAL_DATA_DIR / state_abbr
        create_state_issuers(state_dir, issuers_df)
        return state_dir, merged_df
    except KeyError as ke:
        logging.error(f"Key error in merging data: {ke}")
        raise


def split_by_type(merged_issues_df: pd.DataFrame) -> dict:
    """
    Splits the DataFrame into separate DataFrames based on 'Issuer Type'.
    """
    issuer_type_dfs = {}
    for issuer_type in ISSUER_TYPES:
        issuer_df = merged_issues_df[merged_issues_df["Issuer Type"] == issuer_type]
        if not issuer_df.empty:
            issuer_type_dfs[issuer_type] = issuer_df
        else:
            logging.info(f"No issues found for {issuer_type}.")
    return issuer_type_dfs


def merge_and_split_by_state():
    """
    Processes data for each state: merges, splits by issuer type, and saves to CSV.
    Custom handling for 'state' type issuers to drop specific columns before saving.
    """
    for state_abbr, _ in STATE_LOCATORS:
        state_dir, merged_df = merge_financial_data(state_abbr)
        issuers_dict = split_by_type(merged_df)
        for k, v in issuers_dict.items():
            output_name = state_dir / f"merged_{str(k).lower()}_issuers.csv"
            v.to_csv(output_name, index=False)
            logging.info(f"Saved {k} type issues for {state_abbr} to {output_name}.")


merge_and_split_by_state()
