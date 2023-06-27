import os
import csv
import boto3
import re
import requests
from botocore.exceptions import NoCredentialsError
from bs4 import BeautifulSoup


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


# Download the CSV from AWS S3
download_from_aws('marinasdatabase', 'urls.csv', 'urls.csv')

# Read URLs from CSV
with open('urls.csv', 'r') as f:
  reader = csv.reader(f)
  urls = list(reader)

# Open the output CSV file
with open('marina_data.csv', 'w', newline='') as file:
  writer = csv.writer(file)
  # Write the headers
  writer.writerow([
    "Marina Name", "Phone Number", "Zip Code", "Total Slips",
    "Transient Slips", "Daily Rate", "Weekly Rate", "Monthly Rate",
    "Annual Rate"
  ])

  for url in urls:
    url = url[0].lstrip('\ufeff')
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extracting data
    text = soup.get_text()

    marina_name = re.search(r'(?<=Marina Name: ).*?(?=\n)', text)
    phone_number = re.search(r'(?<=Phone: ).*?(?=\n)', text)
    zip_code = re.search(r'(?<=Zip: ).*?(?=\n)', text)
    total_slips = re.search(r'(?<=Total Slips: ).*?(?=\n)', text)
    transient_slips = re.search(r'(?<=Transient Slips: ).*?(?=\n)', text)
    daily_rate = re.search(r'(?<=Daily: ).*?(?=\n)', text)
    weekly_rate = re.search(r'(?<=Weekly: ).*?(?=\n)', text)
    monthly_rate = re.search(r'(?<=Monthly: ).*?(?=\n)', text)
    annual_rate = re.search(r'(?<=Annual: ).*?(?=\n)', text)

    # Write the data to the CSV
    writer.writerow([
      marina_name.group(0) if marina_name else 'N/A',
      phone_number.group(0) if phone_number else 'N/A',
      zip_code.group(0) if zip_code else 'N/A',
      total_slips.group(0) if total_slips else 'N/A',
      transient_slips.group(0) if transient_slips else 'N/A',
      daily_rate.group(0) if daily_rate else 'N/A',
      weekly_rate.group(0) if weekly_rate else 'N/A',
      monthly_rate.group(0) if monthly_rate else 'N/A',
      annual_rate.group(0) if annual_rate else 'N/A',
    ])

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
