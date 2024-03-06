import csv
import os
import boto3
import requests

def download_from_aws(s3_client, bucket, s3_file, local_file):
    try:
        s3_client.download_file(bucket, s3_file, local_file)
        print("Download Successful")
    except Exception as e:
        print(f"Error downloading from AWS: {e}")
        raise  # Reraise the exception to make sure it's not silently failing

def upload_to_aws(s3_client, local_file, bucket, s3_file):
    try:
        s3_client.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
    except Exception as e:
        print(f"Error uploading to AWS: {e}")
        raise  # Reraise the exception to ensure visibility of failure

def get_zestimate(address):
    url = "https://zillow-working-api.p.rapidapi.com/byaddress"
    headers = {
        "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "zillow-working-api.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params={"propertyaddress": address})

    if response.status_code == 200:
        data = response.json()
        return data.get("zestimate"), data.get("PropertyZillowURL")
    else:
        print(f"Error fetching zestimate for {address}: {response.text}")
        return None, None

def process_addresses(input_file, output_file):
    with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        headers = ["Address", "Zestimate", "PropertyZillowURL"]
        writer.writerow(headers)

        address_count = 0  # Initialize the counter

        for row in reader:
            address = row[0]
            zestimate, url = get_zestimate(address)
            writer.writerow([address, zestimate, url])
            address_count += 1  # Increment the counter
            print(f"Processed address {address_count}: {address}")  # Print progress for each address

        print(f"Processed {address_count} addresses.")  # Print the total number of addresses processed

if __name__ == "__main__":
    print("Script started.")
    bucket_name = 'marinasdatabase'
    input_csv = 'addresses.csv'
    output_csv = 'zesty.csv'
    local_input_csv = '/tmp/addresses.csv'
    local_output_csv = '/tmp/zesty.csv'

    s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    download_from_aws(s3_client, bucket_name, input_csv, local_input_csv)
    process_addresses(local_input_csv, local_output_csv)
    upload_to_aws(s3_client, local_output_csv, bucket_name, output_csv)
    print("Script completed.")
