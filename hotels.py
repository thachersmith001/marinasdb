import os
import csv
import boto3
import requests
from botocore.exceptions import NoCredentialsError

# Function to download file from AWS S3
def download_from_aws(s3_file, local_file, bucket):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
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

# Function to upload file to AWS S3
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

# Function to convert coordinates to decimal format
def convert_to_decimal_degrees(coord):
    # remove degree symbol, split into degrees and minutes
    degrees, minutes = coord.replace('°', '').split('\'')[0:2]
    # convert to decimal format
    decimal_coord = float(degrees) + float(minutes)/60
    return decimal_coord

# Function to fetch hotels from Amadeus API
def fetch_hotels(api_key, api_secret, latitude, longitude):
    token_url = "https://api.amadeus.com/v1/security/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": api_key,
        "client_secret": api_secret
    }
    response = requests.post(token_url, headers=headers, data=data)
    access_token = response.json()["access_token"]

    url = "https://api.amadeus.com/v2/shopping/hotel-offers"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "radius": 10,
        "radiusUnit": "MILE",
        "view": "FULL",
        "sort": "PRICE"
    }
    headers = {"Authorization": "Bearer " + access_token}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        return response.json()["data"]
    else:
        print("Failed to fetch hotels:", response.content)
        return []

# Function to process coordinates and fetch hotel details
def process_coordinates(coordinates):
    api_key = os.environ.get('AMADEUS_API_KEY')
    api_secret = os.environ.get('AMADEUS_API_SECRET')
    hotel_details = []
    for coord in coordinates:
        lat, long = map(convert_to_decimal_degrees, coord)
        hotels = fetch_hotels(api_key, api_secret, lat, long)
        if hotels:
            hotels = sorted(hotels, key=lambda x: x['offers'][0]['price']['total'])
            lowest_price = hotels[0]['offers'][0]['price']['total']
            highest_price = hotels[-1]['offers'][0]['price']['total']
            median_price = hotels[len(hotels)//2]['offers'][0]['price']['total']
            highest_price_name = hotels[-1]['hotel']['name']
            hotel_details.append((highest_price, lowest_price, median_price, highest_price_name))
        else:
            print("No hotels found for coordinates:", coord)
    return hotel_details

def read_coordinates():
    with open('coords.csv', 'r') as file:
        reader = csv.reader(file)
        return list(reader)

def main():
    bucket_name = 'marinasdatabase'
    download_from_aws('coords.csv', 'coords.csv', bucket_name)  # Download coordinates from S3 bucket
    coordinates = read_coordinates()  # Read coordinates from CSV file
    hotel_details = process_coordinates(coordinates)
    with open('hoteldata.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Highest Price", "Lowest Price", "Median Price", "Highest Price Hotel"])
        writer.writerows(hotel_details)
    upload_to_aws('hoteldata.csv', bucket_name, 'hoteldata.csv')  # Upload hotel data to S3 bucket

if __name__ == "__main__":
    main()