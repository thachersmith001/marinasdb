import os
import csv
import boto3
from botocore.exceptions import NoCredentialsError
import requests
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
    marina_name = soup.title.text.split(
      '|')[0].strip() if soup.title else 'N/A'
    phone_number = soup.find(
      text='Reservations').find_next().text if soup.find(
        text='Reservations') else 'N/A'
    zip_code = soup.find(text='Zip:').find_next().text if soup.find(
      text='Zip:') else 'N/A'
    total_slips = soup.find(text='Total Slips:').find_next().text if soup.find(
      text='Total Slips:') else 'N/A'
    transient_slips = soup.find(
      text='Transient Slips:').find_next().text if soup.find(
        text='Transient Slips:') else 'N/A'
    daily_rate = soup.find(text='Daily:').find_next().text if soup.find(
      text='Daily:') else 'N/A'
    weekly_rate = soup.find(text='Weekly:').find_next().text if soup.find(
      text='Weekly:') else 'N/A'
    monthly_rate = soup.find(text='Monthly:').find_next().text if soup.find(
      text='Monthly:') else 'N/A'
    annual_rate = soup.find(text='Annual:').find_next().text if soup.find(
      text='Annual:') else 'N/A'

    # Write the data to the CSV
    writer.writerow([
      marina_name, phone_number, zip_code, total_slips, transient_slips,
      daily_rate, weekly_rate, monthly_rate, annual_rate
    ])

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
