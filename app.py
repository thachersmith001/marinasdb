import os
import csv
import boto3
import re
import requests
import openai
import html2text
from botocore.exceptions import NoCredentialsError


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
    prompt = "Extract the following data from the page:\n"
    prompt += "- Marina Name\n"
    prompt += "- Zip Code\n"
    prompt += "- Daily Rate\n"
    prompt += "- Weekly Rate\n"
    prompt += "- Monthly Rate\n"
    prompt += "- Annual Rate\n"
    prompt += "- Total Slips\n"
    prompt += "- Transient Slips\n"
    prompt += "- Fuel\n"
    prompt += "- Repairs\n"
    prompt += "- Phone Number\n"
    prompt += "- Latitude\n"
    prompt += "- Longitude\n"
    prompt += "- Max Vessel Length\n"

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
    data = response.choices[0].message.content.strip()
    return data


# Download the CSV from AWS S3
download_from_aws('marinasdatabase', 'urls.csv', 'urls.csv')

# Read URLs from CSV
with open('urls.csv', 'r') as f:
    reader = csv.reader(f)
    urls = list(reader)

# Open the output CSV file
with open('marina_data.csv', 'w', newline='') as file:
    writer = csv.writer(file)

    for url in urls:
        url = url[0].lstrip('\ufeff')
        # Use requests to get the content of the page
        response = requests.get(url)
        content = response.text
        # Use the OpenAI API to extract data from the URL
        results = extract_data(content)

        # Extract the required values from the results string using regular expressions
        marina_name = re.search(r"Marina Name: (.+)", results).group(1)
        zip_code = re.search(r"Zip Code: (.+)", results).group(1)
        daily_rate = re.search(r"Daily Rate: (.+)", results).group(1)
        weekly_rate = re.search(r"Weekly Rate: (.+)", results).group(1)
        monthly_rate = re.search(r"Monthly Rate: (.+)", results).group(1)
        annual_rate = re.search(r"Annual Rate: (.+)", results).group(1)
        total_slips = re.search(r"Total Slips: (.+)", results).group(1)
        transient_slips = re.search(r"Transient Slips: (.+)", results).group(1)
        fuel = re.search(r"Fuel: (.+)", results).group(1)
        repairs = re.search(r"Repairs: (.+)", results).group(1)
        phone_number = re.search(r"Phone Number: (.+)", results).group(1)
        latitude = re.search(r"Latitude: (.+)", results).group(1)
        longitude = re.search(r"Longitude: (.+)", results).group(1)
        max_vessel_length = re.search(r"Max Vessel Length: (.+)", results).group(1)

        # Write the extracted data to the CSV
        writer.writerow([
            marina_name.split(": ")[1], zip_code.split(": ")[1], daily_rate.split(": ")[1],
            weekly_rate.split(": ")[1], monthly_rate.split(": ")[1], annual_rate.split(": ")[1],
            total_slips.split(": ")[1], transient_slips.split(": ")[1], fuel.split(": ")[1],
            repairs.split(": ")[1], phone_number.split(": ")[1], latitude.split(": ")[1],
            longitude.split(": ")[1], max_vessel_length.split(": ")[1]
        ])

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
