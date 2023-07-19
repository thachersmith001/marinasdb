import os
import pandas as pd
import requests
import boto3
from statistics import median


def convert_to_decimal(coord, is_longitude=False):
  # If the coordinate is already a float, it's already in decimal format
  if isinstance(coord, float):
    return coord

  coord = coord.strip(
  )  # Remove any leading/trailing spaces from the entire coordinate
  parts = coord.split('° ')
  degrees = parts[0].strip()  # Remove any leading/trailing spaces

  # Check if the coordinate includes seconds
  if '"' in parts[1]:
    minutes, seconds = parts[1].split("' ")
    seconds = seconds.rstrip(
      '"').strip()  # Remove " character and any leading/trailing spaces
  else:
    minutes = parts[1].rstrip(
      "'").strip()  # Remove ' character and any leading/trailing spaces
    seconds = "0"

  try:
    decimal_degrees = float(
      degrees) + float(minutes) / 60 + float(seconds) / 3600
  except ValueError:
    print(f"Failed to convert coordinate: {coord}")
    return None

  # If it's longitude and in North America, make it negative
  if is_longitude:
    decimal_degrees *= -1

  return decimal_degrees


# Load the environment variables
AMADEUS_API_KEY = os.getenv('AMADEUS_API_KEY')
AMADEUS_API_SECRET = os.getenv('AMADEUS_API_SECRET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Radius for the hotel search
RADIUS_MILES = 10

# Create a session using your AWS credentials
session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_REGION)

# Create an S3 client
s3 = session.client('s3')

# Download the file from S3
s3.download_file(S3_BUCKET_NAME, 'coords.csv', 'coords.csv')

# Load the coordinates
df = pd.read_csv('coords.csv', header=None, names=['Latitude', 'Longitude'])

# Convert coordinates to decimal
df['Latitude'] = df['Latitude'].apply(convert_to_decimal)
df['Longitude'] = df['Longitude'].apply(lambda x: convert_to_decimal(x, True))

# Obtain the access token
token_url = "https://api.amadeus.com/v1/security/oauth2/token"
token_data = {
  'grant_type': 'client_credentials',
  'client_id': AMADEUS_API_KEY,
  'client_secret': AMADEUS_API_SECRET
}
token_res = requests.post(token_url, data=token_data)
token = token_res.json()['access_token']

# Prepare the output data
output_data = []

# Iterate over each row in the DataFrame
for _, row in df.iterrows():
  latitude, longitude = row['Latitude'], row['Longitude']

  # Call the Amadeus hotel list API
  hotel_list_url = f"https://api.amadeus.com/v1/reference-data/locations/hotels/by-geocode?latitude={latitude}&longitude={longitude}&radius={RADIUS_MILES}&radiusUnit=MILE"
  hotel_list_res = requests.get(hotel_list_url,
                                headers={'Authorization': f'Bearer {token}'})
  hotel_list_data = hotel_list_res.json()

  # Extract the hotel IDs
  hotel_ids = [hotel['hotelId'] for hotel in hotel_list_data['data']]

  # Call the Amadeus hotel search API
  hotel_search_url = f"https://api.amadeus.com/v3/shopping/hotel-offers?hotelIds={','.join(hotel_ids)}&adults=1&roomQuantity=1&paymentPolicy=NONE&includeClosed=true&bestRateOnly=true"
  hotel_search_res = requests.get(hotel_search_url,
                                  headers={'Authorization': f'Bearer {token}'})
  hotel_search_data = hotel_search_res.json()

  # Extract the hotel prices
  prices = []
  for hotel in hotel_search_data['data']:
    if hotel['available'] and 'offers' in hotel and 'price' in hotel['offers'][
        0] and 'base' in hotel['offers'][0]['price']:
      price = float(hotel['offers'][0]['price']['base'])
      prices.append(price)

  if prices:
    # Calculate the highest, lowest, and median prices
    highest_price = max(prices)
    lowest_price = min(prices)
    median_price = median(prices)

    # Find the name of the highest priced hotel
    highest_priced_hotel = next(
      hotel['hotel']['name'] for hotel in hotel_search_data['data']
      if hotel['available'] and 'offers' in hotel
      and float(hotel['offers'][0]['price']['base']) == highest_price)

    # Add the data to the output
    output_data.append({
      'latitude': latitude,
      'longitude': longitude,
      'highest_price': highest_price,
      'lowest_price': lowest_price,
      'median_price': median_price,
      'highest_priced_hotel': highest_priced_hotel
    })

# Convert the output data to a DataFrame
output_df = pd.DataFrame(output_data)

# Save the output data to a CSV file
output_df.to_csv('demos.csv', index=False)

# Upload the file to S3
s3.upload_file('demos.csv', S3_BUCKET_NAME, 'demos.csv')
