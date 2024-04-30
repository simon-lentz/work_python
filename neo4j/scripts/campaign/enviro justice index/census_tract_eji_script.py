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

def create_filtered_df(df, identifier, index_column=None):
    # Initialize a list to store new columns
    new_columns = []

    # Extract 'County FIPS' and 'Census Tract FIPS', add them to the list
    if index_column:
        df['County FIPS'] = df[index_column].astype(str).str[-11:-6]
        df['Census Tract FIPS'] = df[index_column].astype(str).str[-11:]
        new_columns.append(df[['County FIPS', 'Census Tract FIPS']])

    # Standardize numeric values
    for col in df.columns:
        # Check if the column is numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].mask(df[col] < 0, np.nan)

    # Use filter to select columns that contain the string
    columns_to_keep = df.filter(like=identifier).columns

    # Add the filtered columns to the list
    new_columns.append(df[columns_to_keep])

    # Concatenate all new columns into a new dataframe
    new_df = pd.concat(new_columns, axis=1)

    return new_df

first_pass_df = pd.read_csv(abs_path, dtype={'AFFGEOID':str}, low_memory=False)

first_pass_df.columns = [clean_column_name(col) for col in first_pass_df.columns]

def create_normalized_df(eji_original):
    return create_filtered_df(eji_original, '_', index_column='AFFGEOID')

normalized_df = create_normalized_df(first_pass_df)

normalized_df.to_csv(normalized_path, index=False)

'''
# Read the normalized CSV file
normalized_df = pd.read_csv(normalized_path, dtype={'County FIPS':str, 'Census Tract FIPS':str}, low_memory=False)

# Create the filtered view function and write the partitioned csv files to category dir
filter_params = ['HVM', 'SVM', 'EBM', 'SER', 'EJI']

def process_filter_params(normalized_df, filters):
    # Iterate through the dictionary of filter parameters
    for filter_val in filters:
        # Make sure the category directory exists
        category = f"{filter_val.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}"
        category_dir = os.path.join(os.path.dirname(__file__)+'/category', category)
        os.makedirs(category_dir, exist_ok=True)
        
        filtered_df = create_filtered_df(normalized_df, filter_val)
        
        # Generate the path for the filtered csv
        filtered_path = os.path.join(category_dir, 'temp.csv')
        
        # Save the filtered dataframe to CSV
        filtered_df.to_csv(filtered_path, index=False)
        

process_filter_params(normalized_df, filter_params)
'''