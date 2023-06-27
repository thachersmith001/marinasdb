import os
import csv
import boto3
from botocore.exceptions import NoCredentialsError
import requests
from bs4 import BeautifulSoup


# AWS S3 upload function
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

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
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

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


# Download the CSV from AWS S3
download_from_aws('marinasdatabase', 'urls.csv', 'urls.csv')

# Read URLs from CSV
with open('urls.csv', 'r') as f:
    reader = csv.reader(f)
    urls = list(reader)

# Open the output CSV file
with open('marina_data.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    # Write the headers
    writer.writerow([
        "Total Slips", "Transient Slips", "Daily Rate", "Monthly Rate",
        "Annual Rate", "Marina Manager", "Dockmaster", "Largest Vessel",
        "Dock Type", "Approach / Dockside Depth", "Tide Range", "Liveaboard Allowed",
        "Moorings Offered", "Payment Methods"
    ])

    for url in urls:
        url = url[0].lstrip('\ufeff')
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extracting data
        total_slips = soup.find(text='Total Slips:').find_next().text
        transient_slips = soup.find(text='Transient Slips:').find_next().text
        daily_rate = soup.find(text='Daily:').find_next().text
        monthly_rate = soup.find(text='Monthly:').find_next().text
        annual_rate = soup.find(text='Annual:').find_next().text
        marina_manager = soup.find(text='Marina Manager:').find_next().text
        dockmaster = soup.find(text='Dockmaster:').find_next().text
        largest_vessel = soup.find(text='Largest Vessel:').find_next().text
        dock_type = soup.find(text='Dock Type:').find_next().text
        approach_dockside_depth = soup.find(text='Approach / Dockside Depth:').find_next().text
        tide_range = soup.find(text='Tide Range:').find_next().text
        liveaboard_allowed = soup.find(text='Liveaboard Allowed:').find_next().text
        moorings_offered = soup.find(text='Moorings Offered:').find_next().text
        payment_methods = soup.find(text='Payment Methods:').find_next().text

        # Write the data to the CSV
        writer.writerow([
            total_slips, transient_slips, daily_rate, monthly_rate,
            annual_rate, marina_manager, dockmaster, largest_vessel,
            dock_type, approach_dockside_depth, tide_range, liveaboard_allowed,
            moorings_offered, payment_methods
        ])

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')