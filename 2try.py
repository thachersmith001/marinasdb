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
    except NoCredentialsError:
        print("Credentials not available")

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

def re_geocode_not_found(input_file, output_file):
    geolocator = Nominatim(user_agent="ReGeocoderApp/1.0")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        next(reader)  # Skip header
        writer.writerow(["Address", "Latitude", "Longitude"])

        for row in reader:
            address, lat, lon = row
            if lat == "Not Found" or lon == "Not Found":
                try:
                    location = geocode(address)
                    if location:
                        new_lat, new_lon = location.latitude, location.longitude
                    else:
                        new_lat, new_lon = "Still Not Found", "Still Not Found"
                except Exception as e:
                    print(f"Error re-geocoding {address}: {e}")
                    new_lat, new_lon = "Error", "Error"
            else:
                new_lat, new_lon = lat, lon
            writer.writerow([address, new_lat, new_lon])
            print(f"Processed: {address} -> Lat: {new_lat}, Long: {new_lon}")

if __name__ == "__main__":
    bucket_name = 'marinasdatabase'
    input_csv = 'geocoded.csv'  # The file from S3 to re-process
    output_csv = '/mnt/data/2try.csv'  # Local path for the output file

    # Download the input file from S3
    download_from_aws(bucket_name, input_csv, output_csv)

    # Re-geocode addresses with "Not Found" coordinates
    re_geocode_not_found(output_csv, output_csv)

    # Upload the updated file back to S3
    upload_to_aws(output_csv, bucket_name, '2try.csv')

    print("Re-geocoding complete and file uploaded.")
