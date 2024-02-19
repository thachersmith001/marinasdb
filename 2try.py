import os
import csv
import boto3
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from botocore.exceptions import NoCredentialsError

# Initialize geolocator with a specific user-agent
geolocator = Nominatim(user_agent="validate_us_addresses")

# Define AWS S3 interaction functions
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

def download_from_aws(bucket, s3_file, local_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.download_file(bucket, s3_file, local_file)
        print("Download Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

# Validate and potentially re-geocode addresses with state and country validation
def validate_and_regeocode(input_file, output_file):
    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Validation Result', 'Debug Info']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            address_query = f"{row['Address']}, {row['City']}, {row['State']}, {row['Zip']}, USA"
            location = geolocator.geocode(address_query, exactly_one=True, addressdetails=True)
            if location:
                # Check if the location's state matches the input state
                address_details = location.raw.get('address', {})
                state = address_details.get('state', '').lower()
                country = address_details.get('country', '').lower()
                if row['State'].lower() in state and 'united states' in country:
                    row['Lat'] = location.latitude
                    row['Lon'] = location.longitude
                    row['Validation Result'] = 'Validated'
                else:
                    row['Validation Result'] = 'Mismatch'
                row['Debug Info'] = f"Found state: {state}, country: {country}"
            else:
                row['Validation Result'] = 'Not Found'
                row['Debug Info'] = 'N/A'
            writer.writerow(row)
            print(f"Processed: {address_query} -> {row['Validation Result']}, Debug Info: {row['Debug Info']}")

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'
    local_input_csv = '/tmp/addr.csv'
    local_output_csv = '/tmp/validated.csv'

    # Download, process, and upload
    download_from_aws(bucket_name, 'addr.csv', local_input_csv)
    validate_and_regeocode(local_input_csv, local_output_csv)
    upload_to_aws(local_output_csv, bucket_name, 'validated.csv')
