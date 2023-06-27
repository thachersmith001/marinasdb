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
    "Total Slips", "Transient Slips", "Daily Rate", "Weekly Rate",
    "Monthly Rate", "Annual Rate", "Phone Number", "Website URL", "Owner Name",
    "Manager Name", "Zip Code", "Fuel Available"
  ])

  for url in urls:
    url = url[0].lstrip('\ufeff')
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extracting data
    total_slips_elem = soup.find('tr',
                                 {'style': 'background-color:LightCyan;'})
    total_slips = total_slips_elem.find('td').text if total_slips_elem else ''

    transient_slips_elem = soup.find(
      'tr', {'style': 'background-color:LightYellow;'})
    transient_slips = transient_slips_elem.find(
      'td').text if transient_slips_elem else ''

    daily_rate_elem = soup.find('tr', {'style': 'background-color:LightCyan;'})
    daily_rate = daily_rate_elem.find_next(
      'td').text if daily_rate_elem else ''

    weekly_rate_elem = soup.find('tr',
                                 {'style': 'background-color:LightYellow;'})
    weekly_rate = weekly_rate_elem.find_next(
      'td').text if weekly_rate_elem else ''

    monthly_rate_elem = soup.find('tr',
                                  {'style': 'background-color:LightCyan;'})
    monthly_rate = monthly_rate_elem.find_next(
      'td').text if monthly_rate_elem else ''

    annual_rate_elem = soup.find('tr',
                                 {'style': 'background-color:LightYellow;'})
    annual_rate = annual_rate_elem.find_next(
      'td').text if annual_rate_elem else ''

phone_number_elem = soup.find('div', class_='col-7 col-md-8')
phone_number = phone_number_elem.find('a', class_='phone')['href'][4:].strip(
) if phone_number_elem and phone_number_elem.find('a', class_='phone') else ''

website_url_elem = soup.find('div', class_='col-7 col-md-8')
website_url = website_url_elem.find('a', class_='url')['href'].strip(
) if website_url_elem and website_url_elem.find('a', class_='url') else ''

owner_name_elem = soup.find('h1', class_='marina-owner')
owner_name = owner_name_elem.text.strip() if owner_name_elem else ''

manager_name_elem = soup.find('h3', class_='marina-owner')
manager_name = manager_name_elem.find_next_sibling('h3').text.strip(
) if manager_name_elem and manager_name_elem.find_next_sibling('h3') else ''

zip_code_elem = soup.find('span', class_='postal-code')
zip_code = zip_code_elem.text.strip() if zip_code_elem else ''

fuel_available_elem = soup.find('h4', string='Fuel Available')
fuel_available = fuel_available_elem.find_next('p').text.strip(
) if fuel_available_elem and fuel_available_elem.find_next('p') else ''

# Write the data to the CSV file
writer.writerow([
  total_slips, transient_slips, daily_rate, weekly_rate, monthly_rate,
  annual_rate, phone_number, website_url, owner_name, manager_name, zip_code,
  fuel_available
])

# Remove the downloaded CSV file
os.remove('urls.csv')

# Upload the output CSV file to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
