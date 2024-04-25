import logging
import numpy as np
import pandas as pd
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
FINANCIAL_DATA_DIR = Path.cwd() / "neo4j" / "financial" / "data"
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


def load_bond_data(raw_bonds_file: Path) -> pd.DataFrame:
    try:
        # Read CSV files into DataFrames
        bonds_df = pd.read_csv(
            raw_bonds_file,
            dtype={
                "CUSIP": str,
                "Security Homepage": str,
                "MSRB Security Identifier": str,
                "Principal at Issuance USD": float,
                "Security Description": str,
                "Coupon": float,
                "Initial Offering Price/Yield (%)": float,
                "Initial Offering Price (%)": float,
                "Initial Offering Yield (%)": float,
                "Fitch LT Rating": str,
                "KBRA LT Rating": str,
                "Moody's LT Rating": str,
                "S&P LT Rating": str,
                "Issue Description": str,
                "MSRB Issue Identifier": str,
                "State Abbreviation": str,
                "State FIPS": str
            },
            parse_dates=['Maturity Date', 'Date Retrieved'],
            date_parser=parse_dates,
            na_values="-"
        )
        logging.info(f"Data loaded successfully from {raw_bonds_file}.")
        return bonds_df
    except FileNotFoundError:
        logging.error(f"File not found at {raw_bonds_file}. Please verify the path.")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file at {raw_bonds_file}.")
        raise
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing data from {raw_bonds_file}: {e}")
        raise


def filter_raw_bonds(bonds_df: pd.DataFrame) -> pd.DataFrame:
    # Ensure 'CUSIP' is treated as string, then apply transformation
    bonds_df['CUSIP'] = bonds_df['CUSIP'].astype(str)
    bonds_df['CUSIP'] = np.where(bonds_df['CUSIP'].apply(len) != 9,
                                 "XXXX" + bonds_df['CUSIP'],
                                 bonds_df['CUSIP'])


def sanitize_raw_bonds(state_abbr: str) -> None:
    raw_bonds_file = FINANCIAL_DATA_DIR / state_abbr / "raw_bonds.csv"
    raw_bonds_df = load_bond_data(raw_bonds_file)
    bonds_df = filter_raw_bonds(raw_bonds_df)
    output_path = FINANCIAL_DATA_DIR / state_abbr / "bonds.csv"
    bonds_df.to_csv(output_path, index=False)


def sanitize_by_state():
    for state_abbr, _ in STATE_LOCATORS:
        sanitize_raw_bonds(state_abbr)


sanitize_by_state()
