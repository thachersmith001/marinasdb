import os
import pandas as pd
import requests
import boto3
from statistics import median
import time


def convert_to_decimal(coord, is_longitude=False):
    if isinstance(coord, float):
        return coord
    coord = coord.strip()
    parts = coord.split('° ')
    degrees = parts[0].strip()
    if '"' in parts[1]:
        minutes, seconds = parts[1].split("' ")
        seconds = seconds.rstrip('"').strip()
    else:
        minutes = parts[1].rstrip("'").strip()
        seconds = "0"
    try:
        decimal_degrees = float(
            degrees) + float(minutes) / 60 + float(seconds) / 3600
    except ValueError:
        print(f"Failed to convert coordinate: {coord}")
        return None
    if is_longitude:
        decimal_degrees *= -1
    return decimal_degrees


def get_token():
    token_url = "https://api.amadeus.com/v1/security/oauth2/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': AMADEUS_API_KEY,
        'client_secret': AMADEUS_API_SECRET
    }
    token_res = requests.post(token_url, data=token_data)
    if token_res.status_code != 200:
        print(f"Failed to get token. HTTP status code: {token_res.status_code}")
        return None
    return token_res.json().get('access_token', '')


AMADEUS_API_KEY = os.getenv('AMADEUS_API_KEY')
AMADEUS_API_SECRET = os.getenv('AMADEUS_API_SECRET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

RADIUS_MILES = 15
REQUEST_DELAY = 0.1  # Delay between requests to avoid hitting rate limit
AIRPORT_SEARCH_RADIUS_KM = 80

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_REGION)

s3 = session.client('s3')

s3.download_file(S3_BUCKET_NAME, 'coords.csv', 'coords.csv')

df = pd.read_csv('coords.csv', header=None, names=['Latitude', 'Longitude'])

df['Latitude'] = df['Latitude'].apply(convert_to_decimal)
df['Longitude'] = df['Longitude'].apply(lambda x: convert_to_decimal(x, True))

output_data = []

total_rows = df.shape[0]
print(f"Total rows to process: {total_rows}")

start_time = time.time()

token = get_token()
if not token:
    print("Failed to get token. Exiting program.")
    exit(1)

for i, (_, row) in enumerate(df.iterrows()):
    start_row_time = time.time()
    latitude, longitude = row['Latitude'], row['Longitude']

    airport_search_url = f"https://api.amadeus.com/v1/reference-data/locations/airports?latitude={latitude}&longitude={longitude}&radius={AIRPORT_SEARCH_RADIUS_KM}&page%5Blimit%5D=1&page%5Boffset%5D=0&sort=distance"
    airport_search_res = requests.get(
        airport_search_url, headers={'Authorization': f'Bearer {token}'})
    if airport_search_res.status_code != 200:
        print(f"Error in airport search. HTTP status code: {airport_search_res.status_code}")
        token = get_token()
        output_data.append({
            'latitude': latitude,
            'longitude': longitude,
            'highest_price': 'ERROR',
            'lowest_price': 'ERROR',
            'median_price': 'ERROR',
            'highest_priced_hotel': 'ERROR'
        })
        continue
    airport_search_data = airport_search_res.json()

    if not airport_search_data['data']:
        output_data.append({
            'latitude': latitude,
            'longitude': longitude,
            'highest_price': 'NO AIR',
            'lowest_price': 'NO AIR',
            'median_price': 'NO AIR',
            'highest_priced_hotel': 'NO AIR'
        })
        continue

    closest_airport_code = airport_search_data['data'][0]['iataCode']

    hotel_list_url = f"https://api.amadeus.com/v1/reference-data/locations/pois/by-square?north={latitude+RADIUS_MILES}&west={longitude-RADIUS_MILES}&south={latitude-RADIUS_MILES}&east={longitude+RADIUS_MILES}"
    hotel_list_res = requests.get(hotel_list_url,
                                  headers={'Authorization': f'Bearer {token}'})
    if hotel_list_res.status_code != 200:
        print(f"Error in hotel list. HTTP status code: {hotel_list_res.status_code}")
        token = get_token()
        output_data.append({
            'latitude': latitude,
            'longitude': longitude,
            'highest_price': 'ERROR',
            'lowest_price': 'ERROR',
            'median_price': 'ERROR',
            'highest_priced_hotel': 'ERROR'
        })
        continue
    hotel_list_data = hotel_list_res.json()

    hotel_ids = [
        hotel.get('id', '') for hotel in hotel_list_data.get('data', [])
    ]

    hotel_search_url = f"https://api.amadeus.com/v3/shopping/hotel-offers?hotelIds={','.join(hotel_ids)}&adults=1&roomQuantity=1&paymentPolicy=NONE&includeClosed=true&bestRateOnly=true"
    hotel_search_res = requests.get(hotel_search_url,
                                    headers={'Authorization': f'Bearer {token}'})
    if hotel_search_res.status_code != 200:
        print(f"Error in hotel search. HTTP status code: {hotel_search_res.status_code}")
        token = get_token()
        output_data.append({
            'latitude': latitude,
            'longitude': longitude,
            'highest_price': 'ERROR',
            'lowest_price': 'ERROR',
            'median_price': 'ERROR',
            'highest_priced_hotel': 'ERROR'
        })
        continue
    hotel_search_data = hotel_search_res.json()

    prices = []
    for hotel in hotel_search_data.get('data', []):
        if hotel.get('available') and 'offers' in hotel and 'price' in hotel[
            'offers'][0] and 'total' in hotel['offers'][0]['price']:
            price = float(hotel['offers'][0]['price']['total'])
            prices.append(price)

    if prices:
        highest_price = max(prices)
        lowest_price = min(prices)
        median_price = median(prices)

        highest_priced_hotel = next(
            (hotel['hotel']['name'] for hotel in hotel_search_data.get('data', [])
             if hotel.get('available') and 'offers' in hotel and 'price' in
             hotel['offers'][0] and 'total' in hotel['offers'][0]['price']
             and float(hotel['offers'][0]['price']['total']) == highest_price), 'N/A')

        output_data.append({
            'latitude': latitude,
            'longitude': longitude,
            'highest_price': highest_price,
            'lowest_price': lowest_price,
            'median_price': median_price,
            'highest_priced_hotel': highest_priced_hotel
        })
    else:
        output_data.append({
            'latitude': latitude,
            'longitude': longitude,
            'highest_price': 'N/A',
            'lowest_price': 'N/A',
            'median_price': 'N/A',
            'highest_priced_hotel': 'N/A'
        })

    end_row_time = time.time()
    elapsed_row_time = end_row_time - start_row_time
    elapsed_total_time = end_row_time - start_time
    average_row_time = elapsed_total_time / (i + 1)
    remaining_rows = total_rows - i - 1
    remaining_time = average_row_time * remaining_rows
    print(
        f"Processing row {i+1} of {total_rows}. Estimated remaining time: {remaining_time//60:.0f} minutes {remaining_time%60:.0f} seconds"
    )

    time.sleep(REQUEST_DELAY)

output_df = pd.DataFrame(output_data)

output_df.to_csv('demos.csv', index=False)

s3.upload_file('demos.csv', S3_BUCKET_NAME, 'demos.csv')
