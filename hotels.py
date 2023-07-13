import os
import csv
import boto3
import requests
from botocore.exceptions import NoCredentialsError


def download_from_aws(s3_file, local_file, bucket):
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


def zip_to_lat_long(zip_code):
    api_key = os.environ.get('GOOGLE_API_KEY')
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={zip_code}&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        res = response.json()
        if res['status'] == 'OK':
            lat = res['results'][0]['geometry']['location']['lat']
            lng = res['results'][0]['geometry']['location']['lng']
            return lat, lng
    print(f"Failed to fetch lat-long for zip code {zip_code}:", response.content)
    return None, None


def process_zip_codes(zip_codes):
    api_key = os.environ.get('AMADEUS_API_KEY')
    api_secret = os.environ.get('AMADEUS_API_SECRET')
    hotel_details = []
    for zip_code in zip_codes:
        zip_str = zip_code[0]
        lat, lng = zip_to_lat_long(zip_str)
        if lat and lng:
            hotels = fetch_hotels(api_key, api_secret, lat, lng)
            if hotels:
                hotels = sorted(hotels, key=lambda x: x['offers'][0]['price']['total'])
                lowest_price = hotels[0]['offers'][0]['price']['total']
                highest_price = hotels[-1]['offers'][0]['price']['total']
                median_price = hotels[len(hotels) // 2]['offers'][0]['price']['total']
                highest_price_name = hotels[-1]['hotel']['name']
                hotel_details.append(
                    (highest_price, lowest_price, median_price, highest_price_name))
            else:
                print("No hotels found for zip code:", zip_str)
    return hotel_details


def read_zip_codes():
    with open('zips.csv', 'r', encoding='utf_8_sig') as file:
        reader = csv.reader(file)
        return list(reader)


def main():
    bucket_name = 'marinasdatabase'
    download_from_aws('zips.csv', 'zips.csv', bucket_name)
    zip_codes = read_zip_codes()
    hotel_details = process_zip_codes(zip_codes)
    with open('hoteldata.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Highest Price", "Lowest Price", "Median Price", "Highest Price Hotel"])
        writer.writerows(hotel_details)
    upload_to_aws('hoteldata.csv', bucket_name, 'hoteldata.csv')


if __name__ == "__main__":
    main()
