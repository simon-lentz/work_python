import os
import re
import pandas as pd
import numpy as np

# This pandas option prevents the program from printing
# an unnecessary warning message to stdout when a new string
# is created using a slice of an original string within a df.
pd.set_option('mode.chained_assignment', None) 

# The following path variables only function properly if this script
# is placed in the same directory as the target csv files (original.csv and normalized.csv)
abs_path = os.path.join(os.path.dirname(__file__), "original.csv")
normalized_path = os.path.join(os.path.dirname(__file__), "normalized.csv")

# This should only be run once, during the initial data normalization process.
def clean_column_name(name):
    """
    Cleans a column name by removing double quotes and replacing illegal 
    characters with spaces, except for numeric patterns like "25,000" where the 
    comma is deleted without inserting a space and "USD" is appended.

    Parameters:
    - name (str): The original column name to be cleaned.

    Returns:
    - str: The cleaned column name with illegal characters replaced by spaces 
           and numeric patterns formatted with 'USD', stripped of leading/trailing 
           whitespace.
    """
    # Remove double quotes
    name = name.replace('"', '')
    
    # Detect numeric patterns and transform them by removing the comma and appending 'USD'
    def replace_numeric_pattern(match):
        # Remove commas from the numeric pattern
        number = match.group(0).replace(',', '')
        # Append 'USD' to the pattern
        return f"{number}USD"
    
    # Apply the numeric pattern transformation
    name = re.sub(r'\d{1,3}(?:,\d{3})+', replace_numeric_pattern, name)
    
    # Replace illegal characters (such as commas) with a space for the rest of the text
    name = re.sub(r'[^\w\s]', ' ', name)
    
    # Replace multiple spaces with a single space
    name = re.sub(r'\s+', ' ', name)
    
    return name.strip()

# The following two lines of code should only be run once, as
# the very first step of normalizing the raw data (in the original.csv file)
# df = pd.read_csv(abs_path, dtype={'Census tract 2010 ID':str}, low_memory=False)
# df.columns = [clean_column_name(col) for col in df.columns]
# df.to_csv(normalized_path, index=False)

def create_filtered_df(df, identifier, index_column=None):
    """
    Filters a dataframe to only keep columns related to the given identifier,
    removes unwanted columns, standardizes null values, and extracts an index.

    Parameters:
    - df: The input dataframe.
    - identifier: A string that identifies the columns to keep.
    - index_column: The name of the column containing the foreign key (optional).

    Returns:
    - A new dataframe filtered based on the given identifier.
    """

    # Extract CBSA Code and add as a new column
    df['County FIPS'] = df[index_column].astype(str).str[:5]
    df['Census Tract FIPS'] = df[index_column].astype(str)

    # Standardize numeric values, comment out for burdens indicators.
    #df = df.apply(pd.to_numeric, errors='ignore')

    # Use filter to select columns that contain the string
    columns_to_keep = df.filter(like=identifier).columns

    # Select only these columns in the dataframe
    new_df = df[columns_to_keep]

    # Add CBSA Code column to the new dataframe
    new_df['Census Tract FIPS'] = df['Census Tract FIPS']
    new_df['County FIPS'] = df['County FIPS']
    
    return new_df

test_df = pd.read_csv(normalized_path, dtype={'Census tract 2010 ID': str}, low_memory=False)
filtered_df = create_filtered_df(test_df, 'and is low income', index_column='Census tract 2010 ID')
filtered_path = os.path.join(os.path.dirname(__file__), "temp.csv")
filtered_df.to_csv(filtered_path, index=False)
