import os
import csv
import boto3
import re
import requests
import openai
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

# Function to extract data from the page using OpenAI API
def extract_data(page_content):
    # Initialize the OpenAI API with your API key
    openai.api_key = os.environ.get('OPENAI_API_KEY')

    # Use the OpenAI API to extract the necessary data from the page content
    response = openai.Completion.create(
      engine="text-davinci-003",
      prompt=page_content,
      temperature=0.5,
      max_tokens=200
    )

    # Parse the response to extract the necessary data
    data = response.choices[0].text.strip()
    return data

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
        "Marina Name", "Zip Code", "Daily Rate", "Weekly Rate", "Monthly Rate", "Total Slips", "Transient Slips", "Fuel", "Repairs"
    ])

    for url in urls:
        url = url[0].lstrip('\ufeff')
        # Use requests to get the content of the page
        response = requests.get(url)
        content = response.text
        # Use the OpenAI API to extract data from the URL
        results = extract_data(content)
        # Write the data to the CSV
        writer.writerow(results)

# Close the CSV file
file.close()

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
