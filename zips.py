import os
import csv
import sys
import boto3
import requests
from xml.etree import ElementTree
from botocore.exceptions import NoCredentialsError

# USPS API integration function
def get_zip_code_from_usps(address, city, state):
    user_id = os.environ.get('USPS_API_KEY')
    url = "http://production.shippingapis.com/ShippingAPI.dll"
    xml_request = f"""<ZipCodeLookupRequest USERID="{user_id}">
        <Address ID="0">
            <Address1></Address1>
            <Address2>{address}</Address2>
            <City>{city}</City>
            <State>{state}</State>
        </Address>
    </ZipCodeLookupRequest>"""

    params = {'API': 'ZipCodeLookup', 'XML': xml_request}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        try:
            tree = ElementTree.fromstring(response.content)
            zip_code = tree.find('.//Zip5').text
            return zip_code
        except ElementTree.ParseError:
            print("Error parsing USPS response XML")
            return None
    else:
        print("Failed to get response from USPS API")
        return None

# AWS S3 upload function
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
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
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
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
download_from_aws('marinasdatabase', 'zips.csv', 'zips.csv')

# Process addresses from CSV and query USPS for zip codes
with open('zips.csv', 'r') as input_file, open('addresses_with_zip.csv', 'w', newline='') as output_file:
    csv_reader = csv.reader(input_file)
    csv_writer = csv.writer(output_file)
    csv_writer.writerow(["Address", "City", "State", "Zip Code"])

    for row in csv_reader:
        if row:  # Ensure row is not empty
            address, city, state = row
            zip_code = get_zip_code_from_usps(address, city, state)
            if zip_code:
                csv_writer.writerow([address, city, state, zip_code])
                print(f"Processed {address}, {city}, {state} - Found Zip: {zip_code}")
            else:
                print(f"Failed to find zip code for {address}, {city}, {state}")

# Upload the CSV to AWS S3
upload_to_aws('addresses_with_zip.csv', 'marinasdatabase', 'addresses_with_zip.csv')

# Exit the program
sys.exit()