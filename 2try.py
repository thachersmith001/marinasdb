import os
import csv
import time
import boto3
from geopy.geocoders import Nominatim
from botocore.exceptions import NoCredentialsError
from geopy.exc import GeocoderQueryError

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def download_from_aws(bucket, s3_file, local_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.download_file(bucket, s3_file, local_file)
        print("Download Successful")
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def validate_state(location, input_state):
    if location:
        # Extract the state from the location's raw data
        address_components = location.raw.get('address', {})
        location_state = address_components.get('state', '').upper()
        return location_state == input_state.upper()
    return False

def validate_and_regeocode(input_file, output_file):
    geolocator = Nominatim(user_agent="validate_and_regeocode", timeout=10)
    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Geocode Result', 'Debug Info']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            time.sleep(1)  # Respect Nominatim's request limit
            address = f"{row['Address']}, {row['City']}, {row['State']}, {row['Zip']}, USA"
            try:
                location = geolocator.geocode(address)
                if location and validate_state(location, row['State']):
                    row['Lat'] = location.latitude
                    row['Lon'] = location.longitude
                    row['Geocode Result'] = 'Found'
                    row['Debug Info'] = f"State Matched: {row['State']}"
                else:
                    row['Geocode Result'] = 'Not Found'
                    row['Debug Info'] = 'State mismatch or no geocode result'
            except GeocoderQueryError as e:
                row['Geocode Result'] = 'Error'
                row['Debug Info'] = str(e)
            writer.writerow(row)
            print(f"Processed: {address} -> {row['Geocode Result']}, Debug Info: {row['Debug Info']}")

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'
    input_csv = 'addr.csv'
    output_csv = 'validated.csv'
    local_input_csv = '/tmp/addr.csv'
    local_output_csv = '/tmp/validated.csv'

    download_from_aws(bucket_name, input_csv, local_input_csv)
    validate_and_regeocode(local_input_csv, local_output_csv)
    upload_to_aws(local_output_csv, bucket_name, output_csv)
