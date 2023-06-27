import os
import csv
import boto3
from botocore.exceptions import NoCredentialsError
from autoscraper import AutoScraper



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

# Define a list of sample data to train the scraper
sample_data = ['Bohicket Marina & Market', '+1 (843) 768-1280', '29455', '200', '25', '$3.50 per ft.', 'N/A', '$35.00 per ft.', '$19.50 per ft.']

# Create a new AutoScraper
scraper = AutoScraper()

# Train the scraper on the sample data
scraper.build('https://www.waterwayguide.com/marina/bohicket-marina-and-yacht-club', sample_data)

# Open the output CSV file
with open('marina_data.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    # Write the headers
    writer.writerow([
        "Marina Name", "Phone Number", "Zip Code", "Total Slips",
        "Transient Slips", "Daily Rate", "Weekly Rate", "Monthly Rate",
        "Annual Rate"
    ])

    for url in urls:
        url = url[0].lstrip('\ufeff')
        # Use the scraper to extract data from the URL
        results = scraper.get_result_similar(url, grouped=False)
        # Write the data to the CSV
        writer.writerow(results)

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
