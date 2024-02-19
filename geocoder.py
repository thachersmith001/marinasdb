import os
import csv
import sys
import boto3
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from botocore.exceptions import NoCredentialsError

def geocode_address(address):
    geolocator = Nominatim(user_agent="GeoCoderApp/1.0")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)  # Respect Nominatim's rate limit
    try:
        location = geocode(address)
        if location:
            return location.latitude, location.longitude
        else:
            return "Not Found", "Not Found"
    except Exception as e:
        print(f"Error during geocoding for {address}: {e}")
        return "Error", "Error"

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

def process_addresses(input_file, output_file):
    download_from_aws('marinasdatabase', input_file, input_file)  # Use the provided bucket name

    with open(input_file, mode='r', encoding='utf-8-sig') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(["Address", "Latitude", "Longitude"])

        for i, row in enumerate(reader, 1):
            if not row:  # Skip empty rows
                continue
            address = row[0]
            latitude, longitude = geocode_address(address)
            writer.writerow([address, latitude, longitude])
            print(f"Processed {i}: {address} -> Lat: {latitude}, Long: {longitude}")

    upload_to_aws(output_file, 'marinasdatabase', 'geocoded.csv')  # Uploads the result to the specified bucket

if __name__ == "__main__":
    input_csv = 'addr.csv'  # Assuming the file is in the root directory where the script runs
    output_csv = 'geocoded.csv'  # Output file name
    process_addresses(input_csv, output_csv)
    print("All addresses have been processed and geocoded.")
    sys.exit()
