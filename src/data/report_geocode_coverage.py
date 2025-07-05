import pandas as pd
import sqlite3
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
input_csv = os.path.join(data_dir, 'masterprovider_with_penetration.csv')
db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
table_name = 'geocoded_addresses_new'

cols = ['ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']

google_api = "AIzaSyC_SfnvEo-dCR2F1Z_N1pds3aQJkyowstI"

# Unique addresses in masterprovider_with_penetration.csv (filtered to HHA BRANCH)
df = pd.read_csv(input_csv, dtype=str)
if 'PRACTICE LOCATION TYPE' not in df.columns:
    raise ValueError("Column 'PRACTICE LOCATION TYPE' not found in input file.")
df = df[df['PRACTICE LOCATION TYPE'] == 'HHA BRANCH']
df['ZIP CODE'] = df['ZIP CODE'].astype(str).str[:5].str.zfill(5)
unique_master = df[cols].drop_duplicates()
print(f"Unique addresses in masterprovider_with_penetration.csv (HHA BRANCH only): {len(unique_master)}")

# Unique geocoded addresses in DB (lat/lon not null)
conn = sqlite3.connect(db_path)
geocoded = pd.read_sql(f"SELECT * FROM {table_name}", conn)
conn.close()
geocoded = geocoded[geocoded['lat'].notna() & geocoded['lon'].notna() & (geocoded['lat'] != '') & (geocoded['lon'] != '')]
unique_geocoded = geocoded[cols].drop_duplicates()
print(f"Unique addresses successfully geocoded (lat/lon present): {len(unique_geocoded)}")

# Coverage report
coverage = 100.0 * len(unique_geocoded) / len(unique_master) if len(unique_master) else 0
print(f"Geocode coverage: {len(unique_geocoded)}/{len(unique_master)} ({coverage:.2f}%)")

# Export addresses that could not be geocoded
not_geocoded = pd.merge(unique_master, unique_geocoded, on=cols, how='left', indicator=True)
not_geocoded = not_geocoded[not_geocoded['_merge'] == 'left_only'][cols]
not_geocoded_csv = os.path.join(data_dir, 'addresses_not_geocoded.csv')
not_geocoded.to_csv(not_geocoded_csv, index=False)
print(f"Exported {len(not_geocoded)} addresses not geocoded to {not_geocoded_csv}")

# # Geocoding success rate: % of all geocoding attempts that succeeded (lat/lon present)
# total_attempts = len(geocoded)
# successes = geocoded[geocoded['lat'].notna() & geocoded['lon'].notna() & (geocoded['lat'] != '') & (geocoded['lon'] != '')]
# success_rate = 100.0 * len(successes) / total_attempts if total_attempts else 0
# print(f"Geocoding success rate: {len(successes)}/{total_attempts} ({success_rate:.2f}%)")

# # Geocoding success rate (percentage of addresses in the DB that have lat/lon)
# total_in_db = len(geocoded[cols].drop_duplicates())
# success_in_db = len(unique_geocoded)
# success_rate = 100.0 * success_in_db / total_in_db if total_in_db else 0
# print(f"Geocoding success rate (in DB): {success_in_db}/{total_in_db} ({success_rate:.2f}%)")
