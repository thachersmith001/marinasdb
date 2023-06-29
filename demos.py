import os
import csv
import boto3
import pandas as pd
import censusdata
from botocore.exceptions import NoCredentialsError

# Specify your state
state = 'Florida'  

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
    counties = [row[0].strip().upper() for row in reader]

# Fetch census data for 2020
data_2020 = censusdata.download('acs5', 2020,
    censusdata.censusgeo([('state', '12'), ('county', '*')]),  
    ['B01003_001E'])

data_2020.columns = ['Population_2020']
data_2020.index = data_2020.index.map(lambda x: x.name.split(':')[1].strip().upper())

# Fetch census data for 2021
data_2021 = censusdata.download('acs5', 2021,
    censusdata.censusgeo([('state', '12'), ('county', '*')]),  
    ['B01003_001E', 'B25077_001E', 'B19013_001E', 'B01002_001E'])  # population, median home value, household income, median age

data_2021.columns = ['Population_2021', 'Median Home Value', 'Avg HH Income', 'Avg Age']
data_2021.index = data_2021.index.map(lambda x: x.name.split(':')[1].strip().upper())

# Merge data from 2020 and 2021 on the index (county names)
data = pd.merge(data_2020, data_2021, left_index=True, right_index=True)

# Filter data for specified counties
data = data.loc[data.index.intersection(counties)]

# Calculate YOY population growth
data['YOY Growth'] = (data['Population_2021'] - data['Population_2020']) / data['Population_2020']

# Save data to the output file
data.to_csv(output_file)

# Upload the output file to S3
s3.upload_file(output_file, bucket, output_file)
