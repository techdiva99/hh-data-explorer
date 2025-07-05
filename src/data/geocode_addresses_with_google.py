import pandas as pd
import requests
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data/processed')
input_csv = os.path.join(data_dir, 'addresses_not_geocoded.csv')
output_csv = os.path.join(data_dir, 'addresses_geocoded_google.csv')

google_api = os.getenv('GOOGLE_API_KEY')
if not google_api:
    raise ValueError("GOOGLE_API_KEY not set in environment or .env file")

cols = ['ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']

def build_address(row):
    # Build a single-line address for Google API
    return f"{row['ADDRESS LINE 1']}, {row['CITY']}, {row['STATE']} {row['ZIP CODE']}"

def geocode_address(address, api_key):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        'address': address,
        'key': api_key
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'OK' and data.get('results'):
                loc = data['results'][0]['geometry']['location']
                return loc.get('lat'), loc.get('lng')
    except Exception as e:
        print(f"Error geocoding {address}: {e}")
    return None, None

df = pd.read_csv(input_csv, dtype=str)

results = []
fail_count = 0
max_fails = 10
for idx, row in df.iterrows():
    address = build_address(row)
    lat, lon = geocode_address(address, google_api)
    result = row.to_dict()
    result['lat'] = lat
    result['lon'] = lon
    results.append(result)
    if lat is None or lon is None:
        fail_count += 1
        print(f"{idx+1}/{len(df)}: {address} => FAILED (consecutive fails: {fail_count})")
        if fail_count >= max_fails:
            print(f"Stopping: {fail_count} consecutive addresses could not be geocoded.")
            break
    else:
        print(f"{idx+1}/{len(df)}: {address} => lat: {lat}, lon: {lon}")
        fail_count = 0
    time.sleep(0.25)  # Respect Google API rate limits

results_df = pd.DataFrame(results)
results_df.to_csv(output_csv, index=False)
print(f"Saved geocoded results to {output_csv}")
