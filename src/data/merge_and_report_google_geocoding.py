import pandas as pd
import sqlite3
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
existing_table = 'geocoded_addresses_new'
output_table = 'geocoded_addresses_fin'
google_csv = os.path.join(data_dir, 'addresses_geocoded_google.csv')

cols = ['ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']

# 1. Load Google geocoded results
if not os.path.exists(google_csv):
    raise FileNotFoundError(f"{google_csv} not found. Run Google geocoding first.")
google_df = pd.read_csv(google_csv, dtype=str)
google_df['lat'] = pd.to_numeric(google_df['lat'], errors='coerce')
google_df['lon'] = pd.to_numeric(google_df['lon'], errors='coerce')

# 2. Connect to DB and load existing geocoded addresses
db = sqlite3.connect(db_path)
existing_df = pd.read_sql(f"SELECT * FROM {existing_table}", db)
existing_df['lat'] = pd.to_numeric(existing_df['lat'], errors='coerce')
existing_df['lon'] = pd.to_numeric(existing_df['lon'], errors='coerce')

# 3. Merge: update missing lat/lon with Google geocoded values
merged = existing_df.merge(
    google_df[cols + ['lat', 'lon']],
    on=cols,
    how='left',
    suffixes=('', '_google')
)

# Add update flag and fill missing lat/lon
updated_by_google = []
final_lat = []
final_lon = []
for idx, row in merged.iterrows():
    if pd.notnull(row['lat']) and pd.notnull(row['lon']) and row['lat'] != '' and row['lon'] != '':
        # Already geocoded
        final_lat.append(row['lat'])
        final_lon.append(row['lon'])
        updated_by_google.append(False)
    elif pd.notnull(row['lat_google']) and pd.notnull(row['lon_google']):
        # Use Google geocode
        final_lat.append(row['lat_google'])
        final_lon.append(row['lon_google'])
        updated_by_google.append(True)
    else:
        final_lat.append(None)
        final_lon.append(None)
        updated_by_google.append(False)

merged['lat'] = final_lat
merged['lon'] = final_lon
merged['updated_by_google'] = updated_by_google


# 4. Deduplicate and save to new table and CSV
deduped = merged.drop_duplicates(subset=cols)
drop_sql = f"DROP TABLE IF EXISTS {output_table}"
db.execute(drop_sql)
deduped.to_sql(output_table, db, index=False)

# Also export to CSV
deduped_csv = os.path.join(data_dir, 'geocoded_addresses_fin.csv')
deduped.to_csv(deduped_csv, index=False)
print(f"Deduplicated geocoding results saved to {deduped_csv}")

# 5. Quality report for Google geocoding
num_google = sum(updated_by_google)
total_google_attempts = google_df.shape[0]
success_google = google_df[google_df['lat'].notnull() & google_df['lon'].notnull()]
print(f"Google geocoding: {len(success_google)}/{total_google_attempts} ({100.0*len(success_google)/total_google_attempts:.2f}%) addresses successfully geocoded.")

# 6. Final report for all addresses in DB
num_total = merged.shape[0]
success_total = merged[merged['lat'].notnull() & merged['lon'].notnull()]
print(f"Final geocoding: {len(success_total)}/{num_total} ({100.0*len(success_total)/num_total:.2f}%) addresses have lat/lon.")
print(f"Addresses updated by Google API: {num_google}")

db.close()
print(f"Merged geocoding results saved to table '{output_table}' in DB.")
