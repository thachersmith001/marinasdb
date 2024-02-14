import os
import csv
import sys
import boto3
import requests
from xml.etree import ElementTree as ET
from botocore.exceptions import NoCredentialsError

def get_zip_code_from_usps(address, city, state):
    user_id = os.environ.get('USPS_API_KEY')
    url = "https://secure.shippingapis.com/ShippingAPI.dll"
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
            root = ET.fromstring(response.content)
            zip_code = root.find('.//Zip5').text
            return zip_code
        except ET.ParseError as e:
            print(f"Error parsing USPS response XML: {e}")
            return "Error"
    else:
        print(f"Failed to get response from USPS API, status code: {response.status_code}")
        return "Error"

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

def process_addresses():
    download_from_aws('marinasdatabase', 'zips.csv', 'zips.csv')

    with open('zips.csv', mode='r') as infile, open('codedaddress.csv', mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(["Address", "City", "State", "Zip Code"])

        for row in reader:
            if not row:  # Skip empty rows
                continue
            address, city, state = row[0], row[1], row[2]
            zip_code = get_zip_code_from_usps(address, city, state)
            if zip_code != "Error":
                writer.writerow([address, city, state, zip_code])
                print(f"Processed: {address}, {city}, {state} -> ZIP: {zip_code}")
            else:
                print(f"Error processing: {address}, {city}, {state}")

    upload_to_aws('codedaddress.csv', 'marinasdatabase', 'codedaddress.csv')
    print("All addresses processed and uploaded.")

if __name__ == "__main__":
    process_addresses()
    sys.exit()
