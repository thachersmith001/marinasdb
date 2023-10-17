import os
import csv
import boto3
import pandas as pd
import censusdata
from botocore.exceptions import NoCredentialsError

def get_population_2011(state_code, counties):
    data_2011 = censusdata.download('acs5', 2011,
                                    censusdata.censusgeo([('state', state_code), ('county', '*')]),
                                    ['B01003_001E'])  # population
    data_2011.columns = ['Population_2011']
    data_2011.index = data_2011.index.map(lambda x: x.name.split(',')[0].strip().title() if ',' in x.name else x.name.strip().title())
    ordered_data_2011 = pd.DataFrame(index=counties)
    ordered_data_2011 = ordered_data_2011.join(data_2011).dropna()
    return ordered_data_2011

# Specify your state and its code
state = 'North Carolina'
state_code = '37'

# Specify AWS bucket and filenames
bucket = 'marinasdatabase'
input_file = 'countydemo.csv'
output_file = 'demo.csv'

# Setup S3 client
s3 = boto3.client('s3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

# Download the input file from S3
s3.download_file(bucket, input_file, input_file)

# Load county names from the input file
with open(input_file, 'r') as f_in:
    reader = csv.reader(f_in)
    next(reader)  # skip header
    counties = list(set([row[0].replace('  ', ' ').strip().title() for row in reader]))

# Fetch census data for 2021
data_2021 = censusdata.download('acs5', 2021,
                                censusdata.censusgeo([('state', state_code), ('county', '*')]),
                                ['B01003_001E', 'B25077_001E', 'B19013_001E', 'B01002_001E'])
data_2021.columns = ['Population_2021', 'Median Home Value', 'Avg HH Income', 'Avg Age']
data_2021.index = data_2021.index.map(lambda x: x.name.split(',')[0].strip().title() if ',' in x.name else x.name.strip().title())

# Get the 2011 population data
data_2011 = get_population_2011(state_code, counties)

# Combine the 2021 and 2011 data
combined_data = pd.concat([data_2021, data_2011], axis=1)

# Filter data for specified counties and maintain input order
ordered_data = pd.DataFrame(index=counties)
ordered_data = ordered_data.join(combined_data).dropna()

# Save data to the output file
ordered_data.to_csv(output_file)

# Upload the output file to S3
s3.upload_file(output_file, bucket, output_file)

# Debugging prints
print(counties)
print(combined_data)
print(ordered_data)
