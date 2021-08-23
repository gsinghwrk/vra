import pandas as pds
import numpy as np
from sqlalchemy import create_engine

# Read data from sample csv
print('Begin reading csv file')
df = pds.read_csv('data.csv')

# Converting the string to float format
df['new_case'] = df['new_case'].str.replace(',','').astype(float)
df['new_death'] = df['new_death'].str.replace(',','').astype(float)
df['total_cases'] = df['total_cases'].str.replace(',','').astype(float)
df['total_deaths'] = df['total_deaths'].str.replace(',','').astype(float)

# Converting the string to datetime format
df['submission_date'] = pds.to_datetime(df['submission_date'], format='%m/%d/%Y')

# Check and create column covid_cases_rate using df column new_case
print('Check new_case condition')
nc_condition = [
    (df['new_case'] > 50),
    (df['new_case'] > 20) & (df['new_case'] <= 50),
    (df['new_case'] <= 20)
    ]

nc_value = ['HIGH', 'MEDIUM', 'LOW']

df['covid_cases_rate'] = np.select(nc_condition, nc_value)

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
engine = create_engine('postgresql://postgres:xxxxxxx@127.0.0.1', pool_recycle=5432)
df.to_sql('us_covid_sample', engine, if_exists="replace", index=False)

print('Database table us_covid_sample load completed')


