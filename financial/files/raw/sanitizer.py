import os
import pandas as pd

# State
state = "ak"

# File paths
issue_scale = os.path.join(f"{state}_issues_raw.csv")
issue_os = os.path.join(f"{state}_issue_os_raw.csv")

# Read CSV files into DataFrames
issue_scale_df = pd.read_csv(issue_scale)
issue_os_df = pd.read_csv(issue_os)

# Merge the DataFrames on the "MSRB Issue Identifier" column
merged_df = pd.merge(issue_scale_df, issue_os_df[['MSRB Issue Identifier', 'Official Statement']], on='MSRB Issue Identifier', how='left')  # noqa:E501

# Reorder the columns
column_order = [
    'Issue Name', 'Issue Homepage', 'MSRB Issue Identifier',
    'Dated Date', 'Maturity Date', 'Official Statement',
    'Date Retrieved', 'Issuer Name', 'MSRB Issuer Identifier',
    'State Abbreviation', 'State FIPS'
]
merged_df = merged_df[column_order]

# Fill missing "Official Statement" entries with "-"
merged_df['Official Statement'] = merged_df['Official Statement'].fillna('-')

# Drop duplicate rows based on "MSRB Issue Identifier", keeping the first instance
merged_df = merged_df.drop_duplicates(subset=['MSRB Issue Identifier'])

# Write the merged DataFrame to a new CSV file
output_file = os.path.join(f"{state}_merged_issues.csv")
merged_df.to_csv(output_file, index=False)
