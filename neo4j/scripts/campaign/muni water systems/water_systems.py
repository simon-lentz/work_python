import os
import pandas as pd

# input path
csv_path = os.path.join(os.path.dirname(__file__), 'original.csv')
# Read the CSV file
df = pd.read_csv(csv_path, dtype={
                'Water System Name': str,
                'County(s) Served': str,
                'City(s) Served': str,
                'Population Served': float,
                'Primary Water Source Type': str,
                'PWS Activity': str,
                'Water System ID': str
                })

df.rename(columns={
    'County(s) Served': 'County Name',
    'City(s) Served': 'City Served'
}, inplace=True)

# Add 'State Abbreviation' column to modified_df
df['State Abbreviation'] = df['Water System ID'].str[:2]

# Mapping of state abbreviations to state names
state_map = {'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AS': 'American Samoa', 'AZ': 'Arizona', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'GU': 'Guam', 'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
        'MA': 'Massachusetts', 'MD': 'Maryland', 'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota', 'MO': 'Missouri', 'MP': 'Northern Mariana Islands', 'MS': 'Mississippi', 'MT': 'Montana', 'NA': 'National', 'NC': 'North Carolina', 'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'PR': 'Puerto Rico',
        'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 'VI': 'Virgin Islands', 'VT': 'Vermont', 'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming'}

# Add 'State Name' column to modified_df
df['State Name'] = df['State Abbreviation'].map(state_map)

# Initialize a list to store the new rows
new_rows = []

# Iterate by row
for index, row in df.iterrows():
    # Ensure the 'County Name' value is a string
    county_value = row['County Name']
    if pd.notna(county_value) and isinstance(county_value, str):
        # Split the 'County Name' field into individual counties
        counties = county_value.split(',')
        # Process each county name
        counties = [county.strip() for county in counties]
        # Append 'County' to each county name, except for specific cases
        counties = [
            county if 'Parish' in county or row['State Name'] == 'Alaska' else county + ' County'
            for county in counties
        ]
    else:
        # Handle non-string (e.g., NaN) values
        counties = ['Unknown County']

    for county in counties:
        # Create a new row for each county
        new_row = row.copy()
        new_row['County Name'] = county
        new_rows.append(new_row)

# Concatenate all new rows into a DataFrame
modified_df = pd.concat(new_rows, axis=1).transpose()

county_geographic_path = os.path.join(os.path.dirname(__file__), 'county_geographic.csv')  # Replace 'fips_info.csv' with your actual file name

geographic_df = pd.read_csv(county_geographic_path, dtype={'County FIPS': str})

# Create a dictionary mapping from 'State Name + County Name' to 'County FIPS'
fips_map = dict(zip(geographic_df['State Name'] + geographic_df['County Name'], geographic_df['County FIPS']))

# Initialize a new column for 'County FIPS' in modified_df
modified_df['County FIPS'] = ''

# Iterate over modified_df and map 'County FIPS' based on 'State Name + County Name'
for index, row in modified_df.iterrows():
    composite_key = row['State Name'] + row['County Name']
    modified_df.at[index, 'County FIPS'] = fips_map.get(composite_key, 'Unknown')

# Optionally, remove the 'State Abbreviation' column
modified_df.drop('State Abbreviation', axis=1, inplace=True)

# Save the updated DataFrame
output_path = os.path.join(os.path.dirname(__file__), 'water_system/filtered.csv')
modified_df.to_csv(output_path, index=False)