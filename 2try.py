import os
import csv
import boto3
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from botocore.exceptions import NoCredentialsError

def download_from_aws(bucket, s3_file, local_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.download_file(bucket, s3_file, local_file)
        print("Download Successful")
    except Exception as e:
        print(f"Download Error: {e}")
        raise

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except Exception as e:
        print(f"Upload Error: {e}")
        raise

def validate_and_regeocode(input_file, output_file):
    geolocator = Nominatim(user_agent="validate_us_addresses")
    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ["Validation Status"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            try:
                query = f"{row['Address']}, {row['City']}, {row['State']}, {row['Zip']}, USA"
                location = geolocator.geocode(query, country_codes='us', exactly_one=True, timeout=10)
                if location:
                    row["Lat"], row["Lon"] = location.latitude, location.longitude
                    row["Validation Status"] = "Re-Geocoded"
                else:
                    row["Validation Status"] = "Not Found"
                writer.writerow(row)
                print(f"Processed: {query} -> Validation Status: {row['Validation Status']}")
            except Exception as e:
                print(f"Error processing {query}: {e}")
                row["Validation Status"] = "Error"
                writer.writerow(row)

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'
    input_csv = 'addr.csv'
    output_csv = 'validated.csv'
    local_input_csv = '/tmp/addr.csv'
    local_output_csv = '/tmp/validated.csv'

    download_from_aws(bucket_name, input_csv, local_input_csv)
    validate_and_regeocode(local_input_csv, local_output_csv)
    upload_to_aws(local_output_csv, bucket_name, output_csv)
