import csv
import sys
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def geocode_address(address):
    geolocator = Nominatim(user_agent="GeoCoderApp/1.0")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    try:
        location = geocode(address)
        if location:
            return location.latitude, location.longitude
        else:
            return "Not Found", "Not Found"
    except Exception as e:
        print(f"Error during geocoding for {address}: {e}")
        return "Error", "Error"

def process_addresses(input_file, output_file):
    with open(input_file, mode='r', encoding='utf-8-sig') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(["Address", "Latitude", "Longitude"])

        for i, row in enumerate(reader, 1):
            if not row:  # Skip empty rows
                continue
            address = row[0]
            latitude, longitude = geocode_address(address)
            writer.writerow([address, latitude, longitude])
            print(f"Processed {i}: {address} -> Lat: {latitude}, Long: {longitude}")

if __name__ == "__main__":
    input_csv = '/mnt/data/addr.csv'
    output_csv = '/mnt/data/geocoded.csv'
    process_addresses(input_csv, output_csv)
    print("All addresses have been processed and geocoded.")
    sys.exit()
