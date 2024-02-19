import os
import csv
import boto3
import googlemaps
from botocore.exceptions import NoCredentialsError

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

def geocode_address(gmaps, address):
    # Geocode an address with Google Maps
    try:
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            return location['lat'], location['lng'], True
    except Exception as e:
        print(f"Error geocoding {address}: {e}")
    return None, None, False

def validate_and_regeocode(input_file, output_file, gmaps):
    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Lat', 'Lon', 'Geocode Result']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            address = f"{row['Address']}, {row['City']}, {row['State']}, {row['Zip']}, USA"
            lat, lon, found = geocode_address(gmaps, address)
            if found:
                row['Lat'] = lat
                row['Lon'] = lon
                row['Geocode Result'] = 'Found'
            else:
                row['Geocode Result'] = 'Not Found'
            writer.writerow(row)
            print(f"Processed: {address} -> {row['Geocode Result']}")

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'
    input_csv = 'addr.csv'
    output_csv = 'validated.csv'
    local_input_csv = '/tmp/addr.csv'
    local_output_csv = '/tmp/validated.csv'

    # Initialize Google Maps client
    gmaps = googlemaps.Client(key=os.environ['GOOGLE_MAPS_API_KEY'])

    download_from_aws(bucket_name, input_csv, local_input_csv)
    validate_and_regeocode(local_input_csv, local_output_csv, gmaps)
    upload_to_aws(local_output_csv, bucket_name, output_csv)
