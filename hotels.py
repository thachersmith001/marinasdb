import os
import csv
import boto3
import requests
from botocore.exceptions import NoCredentialsError


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

# Function to fetch hotels from Amadeus API
def fetch_hotels(api_key, api_secret, latitude, longitude):
    url = "https://api.amadeus.com/v1/shopping/hotel-offers"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "radius": 10,
        "radiusUnit": "MILE",
        "view": "FULL",
        "sort": "PRICE"
    }
    headers = {"Authorization": "Bearer " + api_key + ":" + api_secret}
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
        lat, long = coord
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
    with open('coord.csv', 'r') as file:
        reader = csv.reader(file)
        return list(reader)

def main():
    coordinates = read_coordinates()  # Read coordinates from CSV file
    hotel_details = process_coordinates(coordinates)
    with open('hotel_details.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Highest Price", "Lowest Price", "Median Price", "Highest Price Hotel"])
        writer.writerows(hotel_details)
    upload_to_aws('hotel_details.csv', 'your_bucket_name', 'hotel_details.csv')

if __name__ == "__main__":
    main()
