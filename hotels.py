import os
import csv
from statistics import mean
import boto3
from botocore.exceptions import NoCredentialsError
from amadeus import Client, ResponseError

s3 = boto3.client('s3')

def dms2dd(s):
    degrees, minutes = s.split('Â°')
    dd = float(degrees) + float(minutes.replace("'", ""))/60
    return dd

def process_coordinates(s):
    coord = s.replace("N", "").replace("W", "-").strip().split(',')
    return [dms2dd(c) for c in coord]

def download_file(bucket, object_name, file_name):
    try:
        s3.download_file(bucket, object_name, file_name)
    except NoCredentialsError:
        print("No AWS credentials found")
        return False
    return True

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except NoCredentialsError:
        print("No AWS credentials found")
        return False
    return True

def get_hotel_prices(lat, lon):
    amadeus = Client(client_id=os.environ['AMADEUS_CLIENT_ID'], client_secret=os.environ['AMADEUS_CLIENT_SECRET'])
    try:
        response = amadeus.shopping.hotel_offers.get(latitude=lat, longitude=lon, radius=10, radiusUnit='MILE')
        prices = [float(hotel['offers'][0]['price']['total']) for hotel in response.data]
        return min(prices), max(prices), mean(prices)
    except ResponseError as error:
        print(error)
        return None, None, None

bucket = 'marinasdatabase'
input_file = 'coordinates.csv'
output_file = 'hotelpricing.csv'

# Download file from S3 bucket
if download_file(bucket, input_file, input_file):
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(['Latitude', 'Longitude', 'MinPrice', 'MaxPrice', 'MeanPrice'])  # write header
        next(reader)  # skip header
        for row in reader:
            lat, lon = process_coordinates(row[0]), process_coordinates(row[1])
            min_price, max_price, mean_price = get_hotel_prices(lat, lon)
            writer.writerow([lat, lon, min_price, max_price, mean_price])

    # Upload file to S3 bucket
    upload_file(output_file, bucket, output_file)
