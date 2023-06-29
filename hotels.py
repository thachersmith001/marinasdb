import os
import csv
import boto3
from botocore.exceptions import NoCredentialsError

s3 = boto3.client('s3')

def dms2dd(s):
    s = ''.join(c for c in s if c.isdigit() or c in ['-', ' ', '°', "'"])

    parts = s.strip().split('°')
    degrees = parts[0].strip()
    if degrees == '':
        return None

    if ' ' in degrees:
        parts = degrees.split(' ')
        degrees = parts[0]
        if len(parts) > 1:
            parts[1] = parts[1] + '°'
            parts = parts[1:] + parts[2:]

    if len(parts) > 1:
        minutes = parts[1].replace("'", "")
        try:
            if degrees != '-' and degrees != '':
                if minutes != '':
                    return float(degrees) + float(minutes) / 60
                else:
                    return float(degrees)
            else:
                print(f"Invalid degrees value: {degrees}")
        except ValueError as e:
            print(f"Error converting degrees and minutes to float: {e}")
            return None
    else:
        try:
            if degrees != '-' and degrees != '':
                return float(degrees)
            else:
                print(f"Invalid degrees value: {degrees}")
        except ValueError as e:
            print(f"Error converting degrees to float: {e}")
            return None

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
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
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
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
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
            if len(row) >= 2:
                lat, lon = process_coordinates(row[0]), process_coordinates(row[1])
                if lat is not None and lon is not None:
                    processed_rows.append([lat, lon])
            else:
                print("Invalid row format: ", row)

    # Now write the processed rows back into the same file
    with open(file_name, 'w', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(['Latitude', 'Longitude'])  
        writer.writerows(processed_rows)

    upload_file(file_name, bucket, file_name)
