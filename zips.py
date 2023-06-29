import os
import csv
import sys
import boto3
import uszipcode
from uszipcode import SearchEngine
from botocore.exceptions import NoCredentialsError

# AWS S3 upload function
def upload_to_aws(local_file, bucket, s3_file):
  s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
  try:
    s3.upload_file(local_file, bucket, s3_file)
    print("Upload Successful")
    return True
  except FileNotFoundError:
    print("The file was not found")
    return False
  except NoCredentialsError:
    print("Credentials not available")
    return False


# AWS S3 download function
def download_from_aws(bucket, s3_file, local_file):
  s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
  try:
    s3.download_file(bucket, s3_file, local_file)
    print("Download Successful")
    return True
  except FileNotFoundError:
    print("The file was not found")
    return False
  except NoCredentialsError:
    print("Credentials not available")
    return False

# Function to find county by zip code
def find_county_by_zip(zip_code):
    search = SearchEngine(simple_zipcode=True) # set simple_zipcode=False to use rich info database
    zipcode = search.by_zipcode(zip_code)
    return zipcode.to_dict()['county']

# Download the CSV from AWS S3
download_from_aws('marinasdatabase', 'zips.csv', 'zips.csv')

# Read Zip Codes from CSV
with open('zips.csv', 'r') as f:
  reader = csv.reader(f)
  zips = list(reader)

# Initialize a counter
counter = 0

# Open the output CSV file
with open('counties.csv', 'w', newline='') as file:
  writer = csv.writer(file)
  writer.writerow(["Zip Code", "County"])

  for zip in zips:
    zip = zip[0].lstrip('\ufeff')
    county = find_county_by_zip(zip)

    # Write the extracted data to the CSV
    writer.writerow([zip, county])

    # Increment the counter and print the progress
    counter += 1
    print(f"Processed {counter} out of {len(zips)} zip codes")

# Upload the CSV to AWS S3
upload_to_aws('counties.csv', 'marinasdatabase', 'counties.csv')

# Exit the program
sys.exit()
