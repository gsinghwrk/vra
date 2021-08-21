import pandas as pds
import numpy as np
from sqlalchemy import create_engine

# Read data from sample csv
print('Begin reading csv file')
df = pds.read_csv('data.csv')

# Data cleanse and convert to float for df column new_case
df['new_case'] = df['new_case'].str.replace(',','').astype(float)

# Check and create column covid_cases_rate using df column new_case
print('Check new_case condition')
nc_condition = [
    (df['new_case'] > 50),
    (df['new_case'] > 20) & (df['new_case'] <= 50),
    (df['new_case'] <= 20)
    ]

nc_value = ['HIGH', 'MEDIUM', 'LOW']

df['covid_cases_rate'] = np.select(nc_condition, nc_value)

# Data cleanse and convert to float for df column new_death
df['new_death'] = df['new_death'].str.replace(',','').astype(float)

# Check and create column covid_deaths_rate using df column new_death
print('Check new_death condition')
nd_condition = [
    (df['new_death'] > 10),
    (df['new_death'] > 5) & (df['new_death'] <= 10),
    (df['new_death'] <= 5)
    ]

nd_value = ['HIGH', 'MEDIUM', 'LOW']

df['covid_deaths_rate'] = np.select(nd_condition, nd_value)

# display updated DataFrame
df.head()

# Load updated DataFrame to PostgreSQL database table
print('Loading to database table us_covid_sample')
engine = create_engine('postgresql://postgres:xxxxxx@127.0.0.1', pool_recycle=5432)
df.to_sql('us_covid_sample', engine, if_exists="replace", index=False)

print('Database table us_covid_sample load completed')


