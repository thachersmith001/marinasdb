import os
import boto3
import pandas as pd
import requests
from lxml import etree

# AWS S3 and Google Maps API configuration
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'YOUR_BUCKET_NAME'
GOOGLE_MAPS_API_KEY = 'YOUR_GOOGLE_MAPS_API_KEY'

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

# Download the Excel file from S3
s3.download_file(BUCKET_NAME, 'Locations Master v0.xlsx', '/tmp/Locations Master v0.xlsx')

# Load the Excel file
xls = pd.ExcelFile('/tmp/Locations Master v0.xlsx')
sheet_names = xls.sheet_names

# Function to geocode addresses using Google Maps API
def geocode_address(address):
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    response = requests.get(base_url, params={'address': address, 'key': GOOGLE_MAPS_API_KEY})
    data = response.json()
    if data['status'] == 'OK':
        location = data['results'][0]['geometry']['location']
        return location['lat'], location['lng']
    else:
        print(f"Failed to geocode address: {address}")
        return None, None

# KML generation
KML_NAMESPACE = "http://www.opengis.net/kml/2.2"
KML = "{%s}" % KML_NAMESPACE
NSMAP = {None: KML_NAMESPACE}

kml = etree.Element(KML + "kml", nsmap=NSMAP)
document = etree.SubElement(kml, KML + "Document")

for sheet_name in sheet_names:
    data = xls.parse(sheet_name)
    folder = etree.SubElement(document, KML + "Folder")
    folder_name = etree.SubElement(folder, KML + "name")
    folder_name.text = sheet_name

    for _, row in data.iterrows():
        lat, lon = geocode_address(row['Address'])
        if lat and lon:
            placemark = etree.SubElement(folder, KML + "Placemark")
            placemark_name = etree.SubElement(placemark, KML + "name")
            placemark_name.text = row['Marina Name']

            description = etree.SubElement(placemark, KML + "description")
            description.text = "Address: " + row['Address']

            point = etree.SubElement(placemark, KML + "Point")
            coordinates = etree.SubElement(point, KML + "coordinates")
            coordinates.text = f"{lon},{lat}"

# Save the KML to a file
kml_content = etree.tostring(kml, pretty_print=True, xml_declaration=True, encoding="UTF-8")
with open('/tmp/marinas.kml', 'wb') as kml_file:
    kml_file.write(kml_content)

# Upload the KML file back to S3
s3.upload_file('/tmp/marinas.kml', BUCKET_NAME, 'marinas.kml')
