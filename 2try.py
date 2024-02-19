import os
import csv
import boto3
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from botocore.exceptions import NoCredentialsError

# Ensure required libraries are installed: boto3, geopy

def download_from_aws(bucket, s3_file, local_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.download_file(bucket, s3_file, local_file)
        print("Download Successful")
    except FileNotFoundError:
        print("The file was not found")
        raise
    except NoCredentialsError:
        print("Credentials not available")
        raise

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
        raise
    except NoCredentialsError:
        print("Credentials not available")
        raise

def validate_and_regeocode(input_file, output_file):
    geolocator = Nominatim(user_agent="validate_us_addresses")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, error_wait_seconds=10, max_retries=2, swallow_exceptions=(GeocoderTimedOut, GeocoderUnavailable))

    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ["Validated", "Validation Message"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            try:
                location = geocode(f"{row['Address']}, {row['City']}, {row['State']} {row['Zip']}, USA", exactly_one=True, timeout=10)
                if location:
                    if row['City'].lower() in location.address.lower() and row['State'].lower() in location.address.lower():
                        validation_message = "Match Found"
                        row["Validated"] = "Yes"
                    else:
                        validation_message = "City/State Mismatch"
                        row["Validated"] = "No"
                else:
                    validation_message = "No Match Found"
                    row["Validated"] = "No"
                row["Validation Message"] = validation_message
                writer.writerow(row)
                print(f"Processed: {row['Address']} -> {validation_message}")
            except Exception as e:
                print(f"Error processing {row['Address']}: {str(e)}")
                row["Validated"] = "Error"
                row["Validation Message"] = str(e)
                writer.writerow(row)

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'  # Update with your S3 bucket name
    input_csv = 'addr.csv'
    output_csv = 'validated.csv'
    local_input_csv = '/tmp/addr.csv'
    local_output_csv = '/tmp/validated.csv'

    download_from_aws(bucket_name, input_csv, local_input_csv)
    validate_and_regeocode(local_input_csv, local_output_csv)
    upload_to_aws(local_output_csv, bucket_name, output_csv)
