import os
import csv
import boto3
import requests
from bs4 import BeautifulSoup
import spacy
from botocore.exceptions import NoCredentialsError

# Load the spaCy model
nlp = spacy.load('en_core_web_sm')

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
        "Marina Name", "Zip Code", "Daily Rate", "Weekly Rate", "Monthly Rate",
        "Total Slips", "Transient Slips", "Fuel", "Repairs"
    ])

    for url in urls:
        url = url[0].lstrip('\ufeff')
        # Make a request to the website
        r = requests.get(url)
        # Use the 'html.parser' to parse the page
        soup = BeautifulSoup(r.content, 'html.parser')
        # Extract the text from the page
        text = soup.get_text()
        # Use the NER model to extract entities
        doc = nlp(text)
        entities = [ent.text for ent in doc.ents]
        # Write the entities to the CSV
        writer.writerow(entities)

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')