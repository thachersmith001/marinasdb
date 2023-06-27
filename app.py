import os
import csv
import boto3
from botocore.exceptions import NoCredentialsError
import scrapy
from scrapy.crawler import CrawlerProcess


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


download_from_aws('marinasdatabase', 'urls.csv', 'urls.csv')

# Read URLs from CSV
with open('urls.csv', 'r') as f:
    reader = csv.reader(f)
    urls = [row[0].lstrip('\ufeff') for row in reader]

class MarinaSpider(scrapy.Spider):
    name = "marina_spider"
    start_urls = urls

    def parse(self, response):
        marina_name = response.css('title::text').get().split('|')[0].strip()
        phone_number = response.xpath('//span[contains(text(), "Reservations")]/following-sibling::span/a/text()').get()
        zip_code = response.xpath('//span[contains(text(), "Zip:")]/following-sibling::span/text()').get()

        data_points = ['Total Slips:', 'Transient Slips:', 'Daily:', 'Weekly:', 'Monthly:', 'Annual:']
        data_values = {data_point: 'N/A' for data_point in data_points}

        for data_point in data_points:
            data_value = response.xpath(f'//span[contains(text(), "{data_point}")]/following-sibling::span/text()').get()
            if data_value:
                data_values[data_point] = data_value

        yield {
            'Marina Name': marina_name,
            'Phone Number': phone_number,
            'Zip Code': zip_code,
            'Total Slips': data_values['Total Slips:'],
            'Transient Slips': data_values['Transient Slips:'],
            'Daily Rate': data_values['Daily:'],
            'Weekly Rate': data_values['Weekly:'],
            'Monthly Rate': data_values['Monthly:'],
            'Annual Rate': data_values['Annual:'],
        }

process = CrawlerProcess(settings={
    "FEEDS": {
        'marina_data.csv': {"format": "csv"},
    },
})

process.crawl(MarinaSpider)
process.start()  # the script will block here until the crawling is finished

# Upload the CSV to AWS S3
upload_to_aws('marina_data.csv', 'marinasdatabase', 'marina_data.csv')