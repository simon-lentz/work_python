import logging
from io import StringIO
import pandas as pd
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
FINANCIAL_DATA_DIR = Path.cwd() / "neo4j" / "data" / "msrb" / "db"
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


def load_and_filter_data(raw_bonds_file: Path):
    logging.info(f"Starting to load data from {raw_bonds_file}")
    try:
        with open(raw_bonds_file, 'r') as file:
            lines = file.readlines()
    except Exception as e:
        logging.error(f"Failed to read from {raw_bonds_file}: {e}")
        raise

    header = lines[0]
    num_columns = header.count(',') + 1
    logging.info(f"Header expects {num_columns} columns")

    correct_lines = []
    incorrect_lines = []

    for line in lines:
        if line.count(',') + 1 == num_columns:
            correct_lines.append(line)
        # elif line.count(',') + 1 > num_columns:
        #    print(line)
        else:
            incorrect_lines.append(line)

    if incorrect_lines:
        logging.warning(f"{len(incorrect_lines)} incorrect lines found in {raw_bonds_file}")

    correct_data = StringIO(''.join(correct_lines))

    try:
        bonds_df = pd.read_csv(correct_data,
                               dtype={"CUSIP": str, "Security Homepage": str, "MSRB Security Identifier": str,
                                      "Principal at Issuance USD": float, "Security Description": str,
                                      "Coupon": float, "Initial Offering Price/Yield (%)": float,
                                      "Initial Offering Price (%)": float, "Initial Offering Yield (%)": float,
                                      "Fitch LT Rating": str, "KBRA LT Rating": str, "Moody's LT Rating": str,
                                      "S&P LT Rating": str, "Issue Description": str, "MSRB Issue Identifier": str,
                                      "Issuer Name": str, "State Abbreviation": str, "State FIPS": str},
                               parse_dates=['Maturity Date', 'Date Retrieved'],
                               na_values="-")

        logging.info("Successfully created DataFrame from correct lines")
    except Exception as e:
        logging.error(f"Failed to read csv to df: {e}")

    errors_df = pd.DataFrame()
    if incorrect_lines:
        incorrect_data = StringIO(''.join(incorrect_lines))
        try:
            errors_df = pd.read_csv(incorrect_data, dtype=str)
        except Exception as e:
            logging.error(f"Failed to create DataFrame from incorrect lines: {e}")
        logging.info(f"Errors DataFrame created with {len(errors_df)} entries")

    return bonds_df, errors_df


def filter_raw_bonds(bonds_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting to filter raw bonds DataFrame")
    try:
        bonds_df['CUSIP'] = bonds_df['CUSIP'].astype(str)
        bonds_df['CUSIP'] = bonds_df['CUSIP'].apply(lambda x: "XXXX" + x if len(x) != 9 else x)
        logging.info("CUSIP values transformed successfully")
    except Exception as e:
        logging.error(f"Error during CUSIP transformation: {e}")
        raise  # Re-raise the exception after logging
    return bonds_df


def sanitize_raw_bonds(state_abbr: str) -> None:
    logging.info(f"Sanitizing bonds data for {state_abbr}")
    raw_bonds_file = FINANCIAL_DATA_DIR / state_abbr / "raw_bonds.csv"
    try:
        bonds_df, errors_df = load_and_filter_data(raw_bonds_file)
        if bonds_df is not None:
            try:
                bonds_df = filter_raw_bonds(bonds_df)
                output_path = FINANCIAL_DATA_DIR / state_abbr / "sanitized_bonds.csv"
                bonds_df.to_csv(output_path, index=False)
                logging.info(f"Sanitized data saved to {output_path}")
            except Exception as e:
                logging.error(f"Failed to filter and save sanitized data for {state_abbr}: {e}")
        else:
            logging.warning(f"No data available to sanitize for {state_abbr}")

        if not errors_df.empty:
            errors_output_path = FINANCIAL_DATA_DIR / state_abbr / "errors_bonds.csv"
            errors_df.to_csv(errors_output_path, index=False)
            logging.info(f"Errors data saved to {errors_output_path}")
        else:
            logging.info(f"No error records found for {state_abbr}")
    except Exception as e:
        logging.error(f"Failed to load and process data for {state_abbr}: {e}")


def sanitize_by_state():
    for state_abbr, _ in STATE_LOCATORS:
        try:
            sanitize_raw_bonds(state_abbr)
        except Exception as e:
            logging.error(f"Failed to sanitize for {state_abbr}: {e}")


sanitize_by_state()
