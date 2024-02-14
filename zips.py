import os
import csv
import sys
import boto3
from botocore.exceptions import NoCredentialsError
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

def get_zip_code_from_address(address, city, state):
    geolocator = Nominatim(user_agent="myGeocoder")
    try:
        location = geolocator.geocode(f"{address}, {city}, {state}", exactly_one=True)
        if location:
            # Attempt to extract the ZIP code from the address
            address_parts = location.address.split(',')
            # The ZIP code is typically towards the end of the address
            zip_code = [part.strip() for part in address_parts if part.strip().isdigit()]
            if zip_code:
                return zip_code[-1]  # Return the last numeric part which is likely the ZIP code
            else:
                return "ZIP Code Not Found"
        else:
            return "Location Not Found"
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        print(f"Geocoding error: {e}")
        return "Geocoding Error"

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
            address, city, state = row
            zip_code = get_zip_code_from_address(address, city, state)
            writer.writerow([address, city, state, zip_code])
            print(f"Processed: {address}, {city}, {state} -> ZIP: {zip_code}")

    upload_to_aws('codedaddress.csv', 'marinasdatabase', 'codedaddress.csv')
    print("All addresses processed and uploaded.")

if __name__ == "__main__":
    process_addresses()
    sys.exit()
