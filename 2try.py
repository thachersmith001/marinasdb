import os
import csv
import time
import boto3
from geopy.geocoders import Nominatim
from botocore.exceptions import NoCredentialsError

# Function to upload a file to an S3 bucket
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

# Function to download a file from an S3 bucket
def download_from_aws(bucket, s3_file, local_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.download_file(bucket, s3_file, local_file)
        print("Download Successful")
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

# Main function to validate and potentially re-geocode addresses
def validate_and_regeocode(input_file, output_file):
    geolocator = Nominatim(user_agent="myGeocoder")
    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Geocode Result', 'Debug Info']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            address = f"{row['Address']}, {row['City']}, {row['State']}, {row['Zip']}, USA"
            try:
                # Delay between requests to respect Nominatim's usage policy
                time.sleep(1)  # Adjust as necessary based on Nominatim's guidelines
                location = geolocator.geocode(address, exactly_one=True, addressdetails=True)
                if location:
                    row['Lat'] = location.latitude
                    row['Lon'] = location.longitude
                    row['Geocode Result'] = 'Found'
                    row['Debug Info'] = f"Lat: {location.latitude}, Lon: {location.longitude}"
                else:
                    row['Geocode Result'] = 'Not Found'
                    row['Debug Info'] = 'N/A'
            except Exception as e:
                row['Geocode Result'] = 'Error'
                row['Debug Info'] = str(e)
            writer.writerow(row)
            print(f"Processed: {address} -> {row['Geocode Result']}, Debug Info: {row['Debug Info']}")

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'  # Example bucket name
    input_csv = 'addr.csv'  # Example input CSV file name
    output_csv = 'validated.csv'  # Example output CSV file name
    local_input_csv = '/tmp/addr.csv'
    local_output_csv = '/tmp/validated.csv'

    # Ensure AWS S3 bucket and file names match your setup
    download_from_aws(bucket_name, input_csv, local_input_csv)
    validate_and_regeocode(local_input_csv, local_output_csv)
    upload_to_aws(local_output_csv, bucket_name, output_csv)
