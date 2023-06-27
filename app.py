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
        "Annual Rate", "Phone Number", "Website URL", "Manager Name",
        "Zip Code"
    ])

    for url in urls:
        url = url[0].lstrip('\ufeff')
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extracting data
        total_slips_elem = soup.find('td', string='Total Slips:')
        total_slips = total_slips_elem.find_next('td').text if total_slips_elem else ''

        transient_slips_elem = soup.find('td', string='Transient Slips:')
        transient_slips = transient_slips_elem.find_next('td').text if transient_slips_elem else ''

        daily_rate_elem = soup.find('td', string='Daily:')
        daily_rate = daily_rate_elem.find_next('td').text if daily_rate_elem else ''

        monthly_rate_elem = soup.find('td', string='Monthly:')
        monthly_rate = monthly_rate_elem.find_next('td').text if monthly_rate_elem else ''

        annual_rate_elem = soup.find('td', string='Annual:')
        annual_rate = annual_rate_elem.find_next('td').text if annual_rate_elem else ''

        phone_number_elem = soup.find('div', class_='col-7 col-md-8')
        phone_number = phone_number_elem.find('a', class_='phone').text if phone_number_elem and phone_number_elem.find('a', class_='phone') else ''

                website_url = website_url_elem.find('a', target='_new')['href'] if website_url_elem and website_url_elem.find('a', target='_new') else ''

        manager_elem = soup.find('td', string='Manager:')
        manager = manager_elem.find_next('td').text if manager_elem else ''

        zip_code_elem = soup.find('td', string='Zip:')
        zip_code = zip_code_elem.find_next('td').text if zip_code_elem else ''

        # Write the data to the CSV
        writer.writerow([
            total_slips, transient_slips, daily_rate, monthly_rate,
            annual_rate, phone_number, website_url, manager, zip_code
        ])

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
