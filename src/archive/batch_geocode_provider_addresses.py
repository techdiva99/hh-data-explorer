import pandas as pd
import os
import time
import requests

# Paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
input_csv = os.path.join(project_root, 'data/processed/masterprovider_with_penetration.csv')
output_csv = os.path.join(project_root, 'data/processed/masterprovider_with_geocodes.csv')

# Read provider addresses and CCN
cols_needed = ['CCN', 'ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']
df = pd.read_csv(input_csv, dtype=str)
print(df.columns)


for col in cols_needed:
    if col not in df.columns:
        raise ValueError(f"Column {col} not found in input file.")

# Standardize ZIP CODE to 5 digits
df['ZIP CODE'] = df['ZIP CODE'].astype(str).str[:5].str.zfill(5)
# Drop duplicates for batch geocoding
unique_addrs = df[cols_needed].drop_duplicates().reset_index(drop=True)
print(f"Found {len(unique_addrs)} unique addresses to geocode.")
print(unique_addrs[0:5])  # Show first 5 unique addresses for debugging
# US Census Geocoder batch endpoint
def census_batch_geocode(addresses):
    # Prepare batch file (address|city|state|zip)
    lines = ["id|address|city|state|zip"]
    for idx, row in addresses.iterrows():
        # Always use 5-digit ZIP
        zip5 = str(row['ZIP CODE'])[:5].zfill(5)
        lines.append(f"{row['CCN']}|{row['ADDRESS LINE 1']}|{row['CITY']}|{row['STATE']}|{zip5}")
    batch_txt = '\n'.join(lines)
    files = {'addressFile': ('addresses.txt', batch_txt)}
    data = {'benchmark': 'Public_AR_Current', 'vintage': 'Current_Current'}
    url = 'https://geocoding.geo.census.gov/geocoder/locations/addressbatch'
    print(f"Submitting {len(addresses)} addresses to US Census batch geocoder...")
    r = requests.post(url, files=files, data=data)
    if r.status_code != 200:
        raise RuntimeError(f"Census geocoder failed: {r.status_code} {r.text}")
    # Response is CSV: id, input address, match status, match type, matched address, lon, lat, ...
    from io import StringIO
    result = pd.read_csv(StringIO(r.text), header=None)
    result.columns = ['CCN', 'input_address', 'match_status', 'match_type', 'matched_address', 'lon', 'lat', 'tiger_line_id', 'side']
    return result[['CCN', 'lat', 'lon', 'match_status', 'matched_address']]

def test_single_address():
    # Take the first address from unique_addrs
    test_addr = unique_addrs.iloc[[0]]
    print("Testing US Census Geocoder with one address:")
    print(test_addr)
    try:
        res = census_batch_geocode(test_addr)
        print("Geocode result:")
        print(res)
    except Exception as e:
        print(f"Test geocode failed: {e}")

if __name__ == "__main__":
    # Test mode: set to True to test one address, False for full batch
    TEST_ONE = True
    if TEST_ONE:
        test_single_address()
    else:
        # Batch geocode in chunks of 1000 (Census limit)
        results = []
        chunk_size = 1000
        for i in range(0, len(unique_addrs), chunk_size):
            chunk = unique_addrs.iloc[i:i+chunk_size]
            try:
                res = census_batch_geocode(chunk)
                results.append(res)
            except Exception as e:
                print(f"Batch {i//chunk_size+1} failed: {e}")
            time.sleep(1)  # Be polite to Census API

        if results:
            geocoded = pd.concat(results, ignore_index=True)
            # Merge back to full provider file
            merged = pd.merge(df, geocoded, on='CCN', how='left')
            merged.to_csv(output_csv, index=False)
            print(f"Wrote geocoded provider file: {output_csv} ({len(merged)} records, {geocoded['lat'].notna().sum()} geocoded)")
        else:
            print("No geocoding results.")
