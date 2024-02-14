import os
import csv
import sys
import boto3
from botocore.exceptions import NoCredentialsError
from rusps import USPSApi, Address

# Function to integrate with USPS API using rusps
def get_zip_code_from_usps(usps_api_key, address_line, city, state):
    usps = USPSApi(usps_api_key, test=False)  # Set test=True for USPS test servers
    address = Address(
        address_1='',
        address_2=address_line,  # USPS expects the main address line here
        city=city,
        state=state,
        zip_code=''  # Leave zip code empty for lookup
    )

    try:
        lookup_response = usps.zipcode_lookup(address)
        zip_code = lookup_response.result['Address']['Zip5']
        return zip_code
    except Exception as e:
        print(f"Error with USPS ZIP code lookup: {e}")
        return "Error"

# AWS S3 upload function
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

# AWS S3 download function
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

# Main function to process addresses and update with ZIP codes
def process_addresses():
    usps_api_key = os.environ.get('USPS_API_KEY')
    if not usps_api_key:
        print("USPS API key is not set.")
        return

    download_from_aws('marinasdatabase', 'zips.csv', 'zips.csv')

    with open('zips.csv', mode='r') as infile, open('codedaddress.csv', mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(["Address", "City", "State", "Zip Code"])  # Assuming these headers

        for row in reader:
            if not row:  # Skip empty rows
                continue
            address, city, state = row[0], row[1], row[2]
            zip_code = get_zip_code_from_usps(usps_api_key, address, city, state)
            writer.writerow([address, city, state, zip_code])
            print(f"Processed: {address}, {city}, {state} -> ZIP: {zip_code}")

    upload_to_aws('codedaddress.csv', 'marinasdatabase', 'codedaddress.csv')
    print("All addresses processed and uploaded.")

if __name__ == "__main__":
    process_addresses()
    sys.exit()
