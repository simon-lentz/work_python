import os
import pandas as pd
import numpy as np

# Load in issue scale raw, coerce datatypes, add
# error prefix "XXXX" to cusip if len(cusip) != 9
# write to new ak_issue_scale.csv file
issue_scale = os.path.join("./alaska_instance/ak_issue_scale_raw.csv")

# Read CSV files into DataFrames
issue_scale_df = pd.read_csv(
    issue_scale,
    dtype={
        "Principal at Issuance USD": float,
        "Coupon": float,
        "Initial Offering Price/Yield (%)": float,
        "Initial Offering Price (%)": float,
        "Initial Offering Yield (%)": float
    },
    na_values="-"
)

# Ensure 'CUSIP' is treated as string, then apply transformation
issue_scale_df['CUSIP'] = issue_scale_df['CUSIP'].astype(str)
issue_scale_df['CUSIP'] = np.where(issue_scale_df['CUSIP'].apply(len) != 9,
                                   "XXXX" + issue_scale_df['CUSIP'],
                                   issue_scale_df['CUSIP'])

# Write the modified DataFrame to a new CSV file
output_path = os.path.join("./alaska_instance/ak_issue_scale.csv")
issue_scale_df.to_csv(output_path, index=False)

# Load in and merge os docs and issues, output merged file
issues = os.path.join("./alaska_instance/ak_issues_raw.csv")
issue_os = os.path.join("./alaska_instance/ak_issue_os_raw.csv")

issues_df = pd.read_csv(issues)
issue_os_df = pd.read_csv(issue_os)

# Merge the DataFrames on the "MSRB Issue Identifier" column
merged_df = pd.merge(issues_df, issue_os_df[['MSRB Issue Identifier', 'Official Statement']], on='MSRB Issue Identifier', how='left')  # noqa:E501

# Reorder the columns
column_order = [
    'Issue Name', 'Issue Homepage', 'MSRB Issue Identifier',
    'Dated Date', 'Maturity Date', 'Official Statement',
    'Date Retrieved', 'Issuer Name', 'MSRB Issuer Identifier',
    'State Abbreviation', 'State FIPS'
]
merged_df = merged_df[column_order]

# Drop duplicate rows based on "MSRB Issue Identifier", keeping the first instance
merged_df = merged_df.drop_duplicates(subset=['MSRB Issue Identifier'])

# Write the merged DataFrame to a new CSV file
output_path = os.path.join("./alaska_instance/ak_issues.csv")
merged_df.to_csv(output_path, index=False)
