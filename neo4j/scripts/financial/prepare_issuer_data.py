import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
FINANCIAL_DATA_DIR = Path.cwd() / "neo4j" / "financial" / "data"
STATE_LOCATORS = (("AK", "02"), ("AL", "01"))
ISSUER_TYPES = ("City", "County", "Other")


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
        issuers_df = load_data(issuers_path)
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
