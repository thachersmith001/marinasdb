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
    prompt = "Extract the following data from the page: Return only the values, without any additional text or the prompt text\n"
    prompt += "- Marina Name\n"
    prompt += "- Zip Code (5 digits)\n"
    prompt += "- Daily Rate (return N/A if not found)\n"
    prompt += "- Weekly Rate (return N/A if not found)\n"
    prompt += "- Monthly Rate (return N/A if not found)\n"
    prompt += "- Annual Rate (return N/A if not found)\n"
    prompt += "- Total Slips\n"
    prompt += "- Transient Slips (return N/A if not found)\n"
    prompt += "- Does the Marina Sell Fuel (Return Y for yes and N for no or if fuel information is not found) \n"
    prompt += "- Does the Marina Offer Repairs (Return Y for yes and N for no) \n"
    prompt += "- Phone Number\n"
    prompt += "- Latitude (convert to DMS)\n"
    prompt += "- Longitude (convert to DMS)\n"
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

    writer.writerow([
        "Marina Name", "Zip Code", "Daily Rate", "Weekly Rate", "Monthly Rate",
        "Annual Rate", "Total Slips", "Transient Slips", "Fuel", "Repairs",
        "Phone Number", "Latitude", "Longitude", "Max Vessel Length"
    ])

    for url in urls:
        url = url[0].lstrip('\ufeff')
        # Use requests to get the content of the page
        response = requests.get(url)
        content = response.text
        # Use the OpenAI API to extract data from the URL
        results = extract_data(content)

        # Extract the required values from the results string using regular expressions
        marina_name_match = re.search(r"Marina Name: (.+)", results)
        marina_name = marina_name_match.group(1) if marina_name_match else ""
        zip_code_match = re.search(r"Zip Code: (.+)", results)
        zip_code = zip_code_match.group(1) if zip_code_match else ""
        daily_rate_match = re.search(r"Daily Rate: (.+)", results)
        daily_rate = daily_rate_match.group(1) if daily_rate_match else ""
        weekly_rate_match = re.search(r"Weekly Rate: (.+)", results)
        weekly_rate = weekly_rate_match.group(1) if weekly_rate_match else ""
        monthly_rate_match = re.search(r"Monthly Rate: (.+)", results)
        monthly_rate = monthly_rate_match.group(1) if monthly_rate_match else ""
        annual_rate_match = re.search(r"Annual Rate: (.+)", results)
        annual_rate = annual_rate_match.group(1) if annual_rate_match else ""
        total_slips_match = re.search(r"Total Slips: (.+)", results)
        total_slips = total_slips_match.group(1) if total_slips_match else ""
        transient_slips_match = re.search(r"Transient Slips: (.+)", results)
        transient_slips = transient_slips_match.group(1) if transient_slips_match else ""
        fuel_match = re.search(r"Fuel: (.+)", results)
        fuel = fuel_match.group(1) if fuel_match else ""
        repairs_match = re.search(r"Repairs: (.+)", results)
        repairs = repairs_match.group(1) if repairs_match else ""
        phone_number_match = re.search(r"Phone Number: (.+)", results)
        phone_number = phone_number_match.group(1) if phone_number_match else ""
        latitude_match = re.search(r"Latitude: (.+)", results)
        latitude = latitude_match.group(1) if latitude_match else ""
        longitude_match = re.search(r"Longitude: (.+)", results)
        longitude = longitude_match.group(1) if longitude_match else ""
        max_vessel_length_match = re.search(r"Max Vessel Length: (.+)", results)
        max_vessel_length = max_vessel_length_match.group(1) if max_vessel_length_match else ""

        # Write the extracted data to the CSV
        writer.writerow([
            marina_name.strip(), zip_code.strip(), daily_rate.strip(),
            weekly_rate.strip(), monthly_rate.strip(), annual_rate.strip(),
            total_slips.strip(), transient_slips.strip(), fuel.strip(),
            repairs.strip(), phone_number.strip(), latitude.strip(),
            longitude.strip(), max_vessel_length.strip()
        ])

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')
