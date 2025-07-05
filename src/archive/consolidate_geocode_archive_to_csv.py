import os
import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
archive_dir = os.path.join(data_dir, 'geocode_archive')
out_csv = os.path.join(data_dir, 'masterprovider_with_geocodes_geopy_2.csv')

# Find all batch files in archive
glob_pattern = 'masterprovider_with_geocodes_geopy_batch_*.csv'
archive_files = [os.path.join(archive_dir, f) for f in os.listdir(archive_dir) if f.startswith('masterprovider_with_geocodes_geopy_batch_') and f.endswith('.csv')]

if not archive_files:
    print('No batch files found in geocode_archive.')
    exit(0)

print(f'Found {len(archive_files)} batch files in geocode_archive.')
all_batches = pd.concat([
    pd.read_csv(f, dtype=str) for f in archive_files
], ignore_index=True)

# Remove duplicates based on CCN, ADDRESS LINE 1, CITY, STATE, ZIP CODE
deduped = all_batches.drop_duplicates(subset=['CCN', 'ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE'])
# Remove rows where lat or lon is missing or empty
filtered = deduped[deduped['lat'].notna() & deduped['lon'].notna() & (deduped['lat'] != '') & (deduped['lon'] != '')]
final_cols = ['CCN', 'ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']
filtered[final_cols].to_csv(out_csv, index=False)
print(filtered[final_cols].columns)
print(f'Wrote consolidated geocode file: {out_csv} ({len(filtered)} unique geocoded records, columns: {final_cols})')

# Add to SQLite DB as table geocoded_addresses
import sqlite3
db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
conn = sqlite3.connect(db_path)
filtered[final_cols].to_sql('geocoded_addresses', conn, if_exists='replace', index=False)
conn.close()
print(f'Loaded {len(filtered)} records into geocoded_addresses table in {db_path}')
