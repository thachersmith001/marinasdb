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
    counties = [row[0].strip() for row in reader]

# Fetch census data
data = censusdata.download('acs5', 2018,
    censusdata.censusgeo([('state', '12'), ('county', '*')]),  
    ['B01003_001E', 'B25077_001E', 'B19013_001E', 'B01002_001E'])  # population, median home value, household income, median age

# Rename columns
data.columns = ['Population', 'Median Home Value', 'Avg HH Income', 'Avg Age']

# Filter data for specified counties and calculate YOY growth (assumed 1% for this example)
data = data.loc[counties]
data['YOY Growth'] = data['Population'] * 0.01  # replace with actual growth rate calculation if available

# Save data to the output file
data.to_csv(output_file)

# Upload the output file to S3
s3.upload_file(output_file, bucket, output_file)
