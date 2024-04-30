import os
import pandas as pd

abs_path = os.path.join(os.path.dirname(__file__), "eji_original.csv")
out_path = os.path.join(os.path.dirname(__file__), "eji_data_node/eji_data_node.csv")

# This options prevents a warning from being printed to stdout each time
# a value is assigned from a slice.
pd.set_option('mode.chained_assignment', None) 

df = pd.read_csv(abs_path, low_memory=False)

# [0]STATEFP,[1]COUNTYFP,[2]TRACTCE,[3]AFFGEOID,[4]GEOID,
# 1,1,20100,1400000US01001020100,1001020100,
df['STATEFP_temp'] = ''
df['COUNTYFP_temp'] = ''
df['TRACTCE_temp'] = ''

# iterrows method is very slow, should probably refactor to vectorized...
for i, row in df.iterrows():
    df['STATEFP_temp'][i] = row[3][9:11]
    df['COUNTYFP_temp'][i] = row[3][9:14]
    df['TRACTCE_temp'][i] = row[3][9:]
    
df['STATEFP'] = df['STATEFP_temp']
df['COUNTYFP'] = df['COUNTYFP_temp']
df['TRACTCE'] = df['TRACTCE_temp']

df = df.drop(columns=['STATEFP_temp', 'COUNTYFP_temp', 'TRACTCE_temp'])

df.columns = df.columns.str.lower()

df.to_csv(out_path, index=False)
