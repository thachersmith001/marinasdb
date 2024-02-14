import os
import csv
import sys
import time
import boto3
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from botocore.exceptions import NoCredentialsError

def get_zip_code_from_address(address, city, state):
    # Unique User-Agent for Nominatim compliance
    geolocator = Nominatim(user_agent="ZipCodeFinder/1.0 (techdog313@gmail.com)")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)  # Ensuring compliance with rate limit

    try:
        location = geocode(f"{address}, {city}, {state}", exactly_one=True)
        if location and location.raw.get('address', {}).get('postcode'):
            return location.raw['address']['postcode']
        else:
            return "ZIP Code Not Found"
    except Exception as e:
        print(f"Error during geocoding: {e}")
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
            time.sleep(1)  # Ensure compliance with the rate limit

    upload_to_aws('codedaddress.csv', 'marinasdatabase', 'codedaddress.csv')
    print("All addresses processed and uploaded.")

if __name__ == "__main__":
    process_addresses()
    sys.exit()
