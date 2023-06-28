import os
import csv
import boto3
import re
import requests
import openai
import html2text
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

    # Convert the HTML content to plaintext
    h = html2text.HTML2Text()
    h.ignore_links = True
    plaintext = h.handle(page_content)

    # Use the OpenAI API to extract the necessary data from the page content
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=plaintext,
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

# Initialize the AWS S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

# Process each URL
for url in urls:
    url = url[0].lstrip('\ufeff')

    # Use requests to get the content of the page
    response = requests.get(url)
    content = response.text

    # Convert the HTML content to plaintext
    h = html2text.HTML2Text()
    h.ignore_links = True
    plaintext = h.handle(content)

    # Save the plaintext content to AWS S3 with the filename as the URL
    filename = re.sub(r'\W+', '', url) + '.txt'
    s3.put_object(Body=plaintext, Bucket='marinasdatabase', Key=filename)
    print(f"Saved plaintext content to {filename}")

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
