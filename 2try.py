import os
import csv
import boto3
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from botocore.exceptions import NoCredentialsError


def download_from_aws(bucket, s3_file, local_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    try:
        s3.download_file(bucket, s3_file, local_file)
        print("Download Successful")
    except FileNotFoundError:
        print("The file was not found")
        exit(1)
    except NoCredentialsError:
        print("Credentials not available")
        exit(1)

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
        exit(1)
    except NoCredentialsError:
        print("Credentials not available")
        exit(1)

def validate_and_regeocode(input_file, output_file):
    geolocator = Nominatim(user_agent="ValidatorApp/1.0")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2)

    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ["Validated Lat", "Validated Lon"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            full_address = f"{row['Address']}, {row['City']}, {row['State']}, {row['Zip']}, USA"
            location = geocode(full_address)
            if location and location.address.find(row['City']) != -1 and location.address.find(row['State']) != -1:
                row["Validated Lat"], row["Validated Lon"] = location.latitude, location.longitude
            else:
                row["Validated Lat"], row["Validated Lon"] = "Not Found", "Not Found"
            writer.writerow(row)
            print(f"Processed: {full_address} -> Lat: {row['Validated Lat']}, Lon: {row['Validated Lon']}")

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'
    input_csv = 'addr.csv'
    local_input_csv = 'addr_downloaded.csv'  # Temporarily store file locally with a different name to avoid conflicts
    local_output_csv = 'validated.csv'

    # Download the input file from S3
    download_from_aws(bucket_name, input_csv, local_input_csv)

    # Validate and possibly re-geocode addresses
    validate_and_regeocode(local_input_csv, local_output_csv)

    # Upload the validated and corrected file back to S3
    upload_to_aws(local_output_csv, bucket_name, 'validated.csv')

    print("Validation and re-geocoding complete. Output uploaded to S3.")
