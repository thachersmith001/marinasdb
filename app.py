import os
import csv
import boto3
from botocore.exceptions import NoCredentialsError
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


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

# Set up Selenium
webdriver_service = Service(ChromeDriverManager().install())
chrome_options = Options()
chrome_options.add_argument(
  "--headless")  # Ensure GUI is off when running on server
driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)

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
    driver.get(url)

    # Extracting data
    marina_name = driver.find_element(By.CSS_SELECTOR,
                                      'h1').text if driver.find_element(
                                        By.CSS_SELECTOR, 'h1') else 'N/A'
    phone_number = driver.find_element(By.CSS_SELECTOR,
                                       'a.phone').text if driver.find_element(
                                         By.CSS_SELECTOR, 'a.phone') else 'N/A'
    zip_code = driver.find_element(
      By.XPATH, '//span[text()="Zip:"]/following-sibling::span'
    ).text if driver.find_element(
      By.XPATH, '//span[text()="Zip:"]/following-sibling::span') else 'N/A'
    total_slips = driver.find_element(
      By.XPATH, '//span[text()="Total Slips:"]/following-sibling::span'
    ).text if driver.find_element(
      By.XPATH,
      '//span[text()="Total Slips:"]/following-sibling::span') else 'N/A'
    transient_slips = driver.find_element(
      By.XPATH, '//span[text()="Transient Slips:"]/following-sibling::span'
    ).text if driver.find_element(
      By.XPATH,
      '//span[text()="Transient Slips:"]/following-sibling::span') else 'N/A'
    daily_rate = driver.find_element(
      By.XPATH, '//span[text()="Daily:"]/following-sibling::span'
    ).text if driver.find_element(
      By.XPATH, '//span[text()="Daily:"]/following-sibling::span') else 'N/A'
    weekly_rate = 'N/A'  # Weekly rate is not provided on the page
    monthly_rate = driver.find_element(
      By.XPATH, '//span[text()="Monthly:"]/following-sibling::span'
    ).text if driver.find_element(
      By.XPATH, '//span[text()="Monthly:"]/following-sibling::span') else 'N/A'
    annual_rate = driver.find_element(
      By.XPATH, '//span[text()="Annual:"]/following-sibling::span'
    ).text if driver.find_element(
      By.XPATH, '//span[text()="Annual:"]/following-sibling::span') else 'N/A'

    # Write the data to the CSV
    writer.writerow([
      marina_name, phone_number, zip_code, total_slips, transient_slips,
      daily_rate, weekly_rate, monthly_rate, annual_rate
    ])

upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
