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

def create_filtered_df(df, identifier, remove_strings=None, cbsa_code_column='Geography'):
    """
    Filters a dataframe to only keep columns related to the given identifier,
    removes unwanted columns, standardizes null values, and extracts CBSA Code.

    Parameters:
    - df: The input dataframe.
    - identifier: A string that identifies the columns to keep.
    - remove_strings: A list of strings for columns to be removed. (optional)
    - cbsa_code_column: The name of the column containing the CBSA Code. (default: 'Geography')

    Returns:
    - A new dataframe filtered based on the given identifier.
    """

    if remove_strings is None:
        remove_strings = ["Annotation of Estimate", "Margin of Error", "Annotation of Margin of Error", "Annotation of"]

    # Extract CBSA Code and add as a new column
    df['CBSA Code'] = df[cbsa_code_column].astype(str).str[-5:]

    # Drop the selected columns from the DataFrame
    columns_to_drop = df.filter(regex='|'.join(remove_strings)).columns
    df.drop(columns_to_drop, axis=1, inplace=True)

    # Standardize null values
    df.replace(['null', '*****', 'N', '(X)'], np.nan, inplace=True)

    # Standardize numeric values
    df = df.apply(pd.to_numeric, errors='ignore')

    # Use filter to select columns that contain the string
    columns_to_keep = df.filter(like=identifier).columns

    # Select only these columns in the dataframe
    new_df = df[columns_to_keep]

    # Add CBSA Code column to the new dataframe
    new_df['CBSA Code'] = df['CBSA Code']

    # Clean the column names by removing the identifier and other unnecessary characters
    new_df.columns = [col.replace(identifier, '').strip() for col in new_df.columns]
    new_df.columns = [col.replace('  ', ' ') for col in new_df.columns]
    
    return new_df

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
# df = pd.read_csv(abs_path, low_memory=False)
# df.columns = [clean_column_name(col) for col in df.columns]
# df.to_csv(normalized_path, index=False)

def process_filter_params(normalized_df, filter_params):
    """
    Processes the filter parameters and writes filtered dataframes to corresponding category directories.

    Parameters:
    - normalized_df (pd.DataFrame): The dataframe to filter.
    - economic_filter_params (list): A list of economic filter parameters.
    - demographic_filter_params (list): A list of demographic filter parameters.
    """

    # Iterate through the dictionary of filter parameters
    for param in filter_params:
        # Make sure the category directory exists
        category = f"{param.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}"
        category_dir = os.path.join(os.path.dirname(__file__)+'/category', category)
        os.makedirs(category_dir, exist_ok=True)
        
        filtered_df = create_filtered_df(normalized_df, param)
        
        # Generate the path for the filtered csv
        filtered_path = os.path.join(category_dir, 'temp.csv')
        
        # Save the filtered dataframe to CSV
        filtered_df.to_csv(filtered_path, index=False)

# Read the normalized CSV file
normalized_df = pd.read_csv(normalized_path, low_memory=False)

# Define economic, demographic, housing, and social filter parameters

housing_filter_params = ['HOUSING OCCUPANCY', 'UNITS IN STRUCTURE', 'YEAR STRUCTURE BUILT',
                         ' ROOMS ', 'BEDROOMS', 'HOUSING TENURE', 'YEAR HOUSEHOLDER MOVED INTO UNIT',
                         'VEHICLES AVAILABLE', 'HOUSE HEATING FUEL', 'SELECTED CHARACTERISTICS',
                         'OCCUPANTS PER ROOM', 'VALUE', 'MORTGAGE STATUS', 'SELECTED MONTHLY OWNER COSTS SMOC',
                         'SELECTED MONTHLY OWNER COSTS AS A PERCENTAGE OF HOUSEHOLD INCOME SMOCAPI', 'GROSS RENT O',
                         'GROSS RENT AS A PERCENTAGE OF HOUSEHOLD INCOME GRAPI']


# Process the filter parameters and create filtered dataframes
process_filter_params(normalized_df, housing_filter_params)

# 11/08/2023
# Be sure to inspect each category's temp.csv file, it is possible
# that some aspect of the dataset has changed and that the above code
# requires some refactoring.
