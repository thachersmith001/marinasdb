import os
import csv
import sys
import boto3
import requests
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

# Function to get address using Google Places API
def get_address(marina_name, city, state):
    API_KEY = os.environ.get('GOOGLE_PLACES_API_KEY')
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?"
    params = {
        "input": f"{marina_name} {city} {state}",
        "inputtype": "textquery",
        "fields": "formatted_address",
        "key": API_KEY
    }
    response = requests.get(base_url, params=params)
    json_response = response.json()
    if json_response["status"] == "OK":
        return json_response["candidates"][0]["formatted_address"]
    else:
        return None

# Download the CSV from AWS S3
download_from_aws('marinasdatabase', 'marinas_rtf_corrected.csv', 'marinas_rtf_corrected.csv')

# Read marinas data from CSV
with open('marinas_rtf_corrected.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # skip the header
    marinas = list(reader)

# Initialize a counter
counter = 0

# Open the output CSV file
with open('safeharbor.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Marina Name", "Address"])

    for marina in marinas:
        marina_name, city, state = marina
        address = get_address(marina_name, city, state)

        # Write the extracted data to the CSV
        writer.writerow([marina_name, address])

        # Increment the counter and print the progress
        counter += 1
        print(f"Processed {counter} out of {len(marinas)} marinas")

# Upload the CSV to AWS S3
upload_to_aws('safeharbor.csv', 'marinasdatabase', 'safeharbor.csv')

# Exit the program
sys.exit()
