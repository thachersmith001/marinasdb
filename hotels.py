import os
import csv
import boto3
from botocore.exceptions import NoCredentialsError

s3 = boto3.client('s3')


def dms2dd(s):
    s = ''.join(c for c in s if c.isdigit() or c in ['-', ' ', '°', "'"])

    parts = s.strip().split('°')
    degrees = parts[0].strip()
    if ' ' in degrees:
        parts = degrees.split(' ')
        degrees = parts[0]
        if len(parts) > 1:
            parts[1] = parts[1] + '°'
            parts = parts[1:] + parts[2:]

    if len(parts) > 1:
        minutes = parts[1].replace("'", "")
        if degrees != '-':
            return float(degrees) + float(minutes) / 60
        else:
            print(f"Invalid degrees value: {degrees}")
    else:
        if degrees != '-':
            return float(degrees)
        else:
            print(f"Invalid degrees value: {degrees}")


def process_coordinates(s):
    coord = s.replace("N", "").replace("W", "-").strip().split(',')
    coord = [c.strip() for c in coord] 
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


bucket = 'marinasdatabase'
file_name = 'coordinates.csv'

if download_file(bucket, file_name, file_name):
    processed_rows = []
    with open(file_name, 'r') as f_in:
        reader = csv.reader(f_in)
        next(reader)  # skip header
        for row in reader:
            lat, lon = process_coordinates(row[0]), process_coordinates(row[1])
            processed_rows.append([lat, lon])

    # Now write the processed rows back into the same file
    with open(file_name, 'w', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(['Latitude', 'Longitude'])  
        writer.writerows(processed_rows)

    upload_file(file_name, bucket, file_name)
