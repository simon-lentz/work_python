import os
import pandas as pd
import numpy as np

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
    identifier_string = f"!!{identifier}!!"
    columns_to_keep = df.filter(like=identifier_string).columns

    # Select only these columns in the dataframe
    new_df = df[columns_to_keep]

    # Add CBSA Code column to the new dataframe
    new_df['CBSA Code'] = df['CBSA Code']

    # Clean the column names by removing the identifier and other unnecessary characters
    new_df.columns = [col.replace(identifier_string, ' ').replace('!!', ' ').strip() for col in new_df.columns]

    return new_df

pd.set_option('mode.chained_assignment', None) 

abs_path = os.path.join(os.path.dirname(__file__), "normalized.csv")
out_path = os.path.join(os.path.dirname(__file__), "categories/temp.csv")

df = pd.read_csv(abs_path, low_memory=False)

#demographic_filter_params = [
#    'HISPANIC OR LATINO AND RACE', 'RACE', 'SEX AND AGE', 'CITIZEN VOTING AGE POPULATION'
#]
# 'HISPANIC OR LATINO AND RACE', 'RACE', 'SEX_AND_AGE', 'CITIZEN VOTING AGE POPULATION'
filtered_df = create_filtered_df(df, 'CITIZEN VOTING AGE POPULATION')

filtered_df.to_csv(out_path, index=False)

