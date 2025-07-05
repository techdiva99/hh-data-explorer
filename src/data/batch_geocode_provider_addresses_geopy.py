import pandas as pd
import os
import time
import sqlite3
import shutil
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


# Paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
archive_dir = os.path.join(data_dir, 'geocode_archive')
os.makedirs(archive_dir, exist_ok=True)
db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
output_csv = os.path.join(data_dir, 'masterprovider_with_geocodes_geopy.csv')
table_name = 'geocoded_addresses_new'

input_csv = os.path.join(data_dir, 'masterprovider_with_penetration.csv')

# Ensure all files in geocode_archive are loaded to DB before starting main loop
def load_archive_to_db():
    archive_files = [f for f in os.listdir(archive_dir) if f.startswith('masterprovider_with_geocodes_geopy_batch_') and f.endswith('.csv')]
    if not archive_files:
        return 0
    all_batches = pd.concat([
        pd.read_csv(os.path.join(archive_dir, f), dtype=str)[['CCN', 'ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE', 'lat', 'lon']]
        for f in archive_files
    ], ignore_index=True)
    conn = sqlite3.connect(db_path)
    all_batches.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    print(f"Loaded {len(all_batches)} records from archive to {table_name} in DB.")
    return len(all_batches)



cols_needed = ['CCN', 'ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']
batch_size = 10
consolidate_every = 10

def get_remaining_addresses():
    master = pd.read_csv(input_csv, dtype=str)
    if 'PRACTICE LOCATION TYPE' not in master.columns:
        raise ValueError("Column 'PRACTICE LOCATION TYPE' not found in input file.")
    master = master[master['PRACTICE LOCATION TYPE'] == 'HHA BRANCH']
    master['ZIP CODE'] = master['ZIP CODE'].astype(str).str[:5].str.zfill(5)
    unique_master = master[cols_needed].drop_duplicates().reset_index(drop=True)
    print(f"Unique addresses to geocode: {len(unique_master)}")
    print(unique_master.head())
    # Get already geocoded addresses from DB
    conn = sqlite3.connect(db_path)
    try:
        geocoded = pd.read_sql(f'SELECT {', '.join(cols_needed)} FROM {table_name}', conn)
    except Exception as e:
        print(f"Warning: {e}. Falling back to empty geocoded DataFrame.")
        geocoded = pd.DataFrame(columns=cols_needed)
    conn.close()
    if geocoded.empty:
        print("No previously geocoded addresses found in DB.")
        return unique_master
    else:
        remaining = pd.merge(unique_master, geocoded, on=cols_needed, how='left', indicator=True)
        remaining = remaining[remaining['_merge'] == 'left_only'][cols_needed]
        print(f"{len(remaining)} addresses remain to be geocoded.")
        print(remaining.head())
        return remaining

def consolidate_batches():
    batch_files = [f for f in os.listdir(data_dir) if f.startswith('masterprovider_with_geocodes_geopy_batch_') and f.endswith('.csv')]
    if not batch_files:
        return 0
    all_batches = pd.concat([pd.read_csv(os.path.join(data_dir, f), dtype=str) for f in batch_files], ignore_index=True)
    conn = sqlite3.connect(db_path)
    all_batches.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    for f in batch_files:
        shutil.move(os.path.join(data_dir, f), os.path.join(archive_dir, f))
    print(f"Consolidated and archived {len(batch_files)} batch files ({len(all_batches)} records).")
    return len(all_batches)

def geocode_batch(batch, batch_idx):
    geolocator = Nominatim(user_agent="hh-data-explorer")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2, swallow_exceptions=False)
    def build_address(row):
        return f"{row['ADDRESS LINE 1']}, {row['CITY']}, {row['STATE']} {row['ZIP CODE']}"
    results = []
    for idx, row in batch.iterrows():
        addr_str = build_address(row)
        print(f"Geocoding: {addr_str}")  # Print before geocoding
        try:
            location = geocode(addr_str, timeout=10)
            lat = location.latitude if location else None
            lon = location.longitude if location else None
            print(f"{idx+1}/{len(batch)}: {addr_str} -> {lat}, {lon}")
        except Exception as e:
            print(f"{idx+1}/{len(batch)}: {addr_str} -> ERROR: {e}")
            lat, lon = None, None
        results.append({
            'CCN': row['CCN'],
            'ADDRESS LINE 1': row['ADDRESS LINE 1'],
            'CITY': row['CITY'],
            'STATE': row['STATE'],
            'ZIP CODE': row['ZIP CODE'],
            'lat': lat,
            'lon': lon
        })
        # For debugging: break after first address
        # break
    geocoded_df = pd.DataFrame(results)
    conn = sqlite3.connect(db_path)
    geocoded_df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    print(f"Appended {len(geocoded_df)} records to {table_name} in DB.")
    batch_output_csv = output_csv.replace('.csv', f'_batch_{batch_idx}.csv')
    geocoded_df.to_csv(batch_output_csv, index=False)
    print(f"Wrote batch geocoded provider file: {batch_output_csv} ({len(geocoded_df)} records, {geocoded_df['lat'].notna().sum()} geocoded)")
    # Also write to DB immediately

if __name__ == "__main__":
    #load_archive_to_db()
    total_geocoded = 0
    while True:
        remaining = get_remaining_addresses()
        print(f"{len(remaining)} addresses remain to be geocoded.")
        if len(remaining) == 0:
            print("All addresses have been geocoded.")
            break
        for i in range(0, len(remaining), batch_size):
            batch = remaining.iloc[i:i+batch_size]
            batch_idx = (total_geocoded // batch_size) + 1
            geocode_batch(batch, batch_idx)
            total_geocoded += len(batch)
            # Consolidate and archive every consolidate_every geocodes
            if total_geocoded % consolidate_every == 0 or (i + batch_size) >= len(remaining):
                consolidate_batches()
    # Final consolidation
    #consolidate_batches()
    print("Geocoding and consolidation process complete.")
