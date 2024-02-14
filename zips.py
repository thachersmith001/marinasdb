import os
import csv
import sys
import time
import boto3
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from botocore.exceptions import NoCredentialsError

def get_zip_code_from_address(address, city, state, retry=0):
    geolocator = Nominatim(user_agent="ZipCodeFinder/1.0")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    full_address = f"{address}, {city}, {state}"
    try:
        location = geocode(full_address, exactly_one=True)
        if location:
            postcode = location.raw.get('address', {}).get('postcode')
            if postcode:
                return postcode.split(';')[0].split('-')[0].strip()
            else:
                # Extract from display_name as fallback
                parts = location.address.split(',')
                for part in parts[::-1]:
                    if part.strip().isdigit():
                        return part.strip()
                return "ZIP Code Not Found"
        else:
            return "Location Not Found"
    except Exception as e:
        if retry < 3:  # Simple retry logic
            time.sleep(2)  # Wait for 2 seconds before retrying
            return get_zip_code_from_address(address, city, state, retry+1)
        else:
            print(f"Error after retries for {full_address}: {e}")
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

    with open('zips.csv', mode='r', encoding='utf-8-sig') as infile:
        rows = list(csv.reader(infile))
        total_addresses = len(rows)

    with open('codedaddress.csv', mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Address", "City", "State", "Zip Code"])

        for i, row in enumerate(rows, 1):
            if not row:  # Skip empty rows
                continue
            address, city, state = row
            zip_code = get_zip_code_from_address(address, city, state)
            writer.writerow([address, city, state, zip_code])
            print(f"Processed {i}/{total_addresses}: {address}, {city}, {state} -> ZIP: {zip_code}")
            time.sleep(1)  # Respect Nominatim's usage policy

    upload_to_aws('codedaddress.csv', 'marinasdatabase', 'codedaddress.csv')
    print("All addresses processed and uploaded.")

if __name__ == "__main__":
    process_addresses()
    sys.exit()
