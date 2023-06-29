import os
import csv
import sys
import boto3
import re
import requests
import openai
import html2text
from botocore.exceptions import NoCredentialsError
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable


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


# Function to truncate the prompt to stay under the input token limit
def truncate_prompt(prompt, max_tokens):
  prompt_tokens = prompt.split(" ")
  if len(prompt_tokens) <= max_tokens:
    return prompt
  truncated_prompt_tokens = prompt_tokens[:max_tokens]
  truncated_prompt = " ".join(truncated_prompt_tokens)
  return truncated_prompt


# Function to extract data from the page using OpenAI API (GPT-3.5-turbo)
def extract_data(page_content):
  # Initialize the OpenAI API with your API key
  openai.api_key = os.environ.get('OPENAI_API_KEY')

  # Convert the HTML content to plaintext
  h = html2text.HTML2Text()
  h.ignore_links = True
  plaintext = h.handle(page_content)

  # Set the maximum tokens for input and completion
  max_input_tokens = 4096
  max_completion_tokens = 4096

  # Adjusted prompt for extracting specific data points
  prompt = "Extract the following data from the page: Return only the values, without any additional text or the prompt text\n"
  prompt += "- Marina Name\n"
  prompt += "- Website\n"
  prompt += "- Zip Code (5 digits)\n"
  prompt += "- Daily Rate Per Foot (value in dollars, return N/A if not found)\n"
  prompt += "- Weekly Rate Per Foot (value in dollars, return N/A if not found)\n"
  prompt += "- Monthly Rate Per Foot (value in dollars, return N/A if not found)\n"
  prompt += "- Annual Rate (value in dollars, return N/A if not found)\n"
  prompt += "- Total Slips\n"
  prompt += "- Transient Slips (return N/A if not found)\n"
  prompt += "- Does the Marina Sell Fuel (Return Y for yes and N for no depending on whether fuel pricing information is present) \n"
  prompt += "- Does the Marina Offer Repairs (Return Y for yes and N for no) \n"
  prompt += "- Phone Number\n"
  prompt += "- Latitude (convert to DMS)\n"
  prompt += "- Longitude (convert to DMS)\n"
  prompt += "- Max Vessel Length (return only value without units) \n"

  # Combine the prompt and plaintext content
  input_text = prompt + plaintext

  # Truncate the prompt if it exceeds the maximum input token limit
  input_text = truncate_prompt(input_text, max_input_tokens)

  # Use the OpenAI API to extract the necessary data from the page content
  response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo-16k",
    messages=[{
      "role": "system",
      "content": "You are a helpful assistant."
    }, {
      "role": "user",
      "content": input_text
    }],
  )

  # Parse the response to extract the necessary data
  data = response.choices[0].message.content.strip().split("\n")
  data_dict = {}
  for line in data:
    if ": " in line:  # Check if the line contains the delimiter
      key, value = line.split(
        ": ", 1)  # Split only at the first occurrence of the delimiter
      data_dict[key] = value
  return data_dict


def convert_to_address(latitude, longitude):
  geolocator = Nominatim(user_agent="myGeocoder")
  location = None
  try:
    location = geolocator.reverse([latitude, longitude], exactly_one=True)
  except (GeocoderTimedOut, GeocoderUnavailable):
    return {"City": "N/A", "State": "N/A", "County": "N/A"}

  address = location.raw['address']
  city = address.get('city', 'N/A')
  state = address.get('state', 'N/A')
  county = address.get('county', 'N/A')

  return {"City": city, "State": state, "County": county}


# Download the CSV from AWS S3
download_from_aws('marinasdatabase', 'urls.csv', 'urls.csv')

# Read URLs from CSV
with open('urls.csv', 'r') as f:
  reader = csv.reader(f)
  urls = list(reader)

# Initialize a counter
counter = 0

# Open the output CSV file
with open('marina_data.csv', 'w', newline='') as file:
  writer = csv.writer(file)

  writer.writerow([
    "Marina Name", "Website", "Zip Code", "Daily Rate", "Weekly Rate",
    "Monthly Rate", "Annual Rate", "Total Slips", "Transient Slips", "Fuel",
    "Repairs", "Phone Number", "Latitude", "Longitude", "Max Vessel Length",
    "City", "State", "County"
  ])

  for url in urls:
    url = url[0].lstrip('\ufeff')
    # Use requests to get the content of the page
    response = requests.get(url)
    content = response.text
    # Use the OpenAI API to extract data from the URL
    results = extract_data(content)

    # Convert the coordinates to city, state, and county
    latitude = float(results.get("Latitude", "0.0"))
    longitude = float(results.get("Longitude", "0.0"))
    location_info = convert_to_address(latitude, longitude)

    # Write the extracted data to the CSV
    writer.writerow([
      results.get("Marina Name", ""),
      results.get("Zip Code", ""),
      results.get("Daily Rate Per Foot", ""),
      results.get("Weekly Rate Per Foot", ""),
      results.get("Monthly Rate Per Foot", ""),
      results.get("Annual Rate", ""),
      results.get("Total Slips", ""),
      results.get("Transient Slips", ""),
      results.get("Does the Marina Sell Fuel", ""),
      results.get("Does the Marina Offer Repairs", ""),
      results.get("Phone Number", ""),
      results.get("Latitude", ""),
      results.get("Longitude", ""),
      results.get("Max Vessel Length", ""),
      location_info.get("City", ""),
      location_info.get("State", ""),
      location_info.get("County", "")
    ])

    # Increment the counter and print the progress
    counter += 1
    print(f"Scraped {counter} out of {len(urls)} pages")

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')

# Stop the Heroku dyno
os.system("heroku ps:scale worker=0 --app your-heroku-app-name")

# Exit the program
sys.exit()
