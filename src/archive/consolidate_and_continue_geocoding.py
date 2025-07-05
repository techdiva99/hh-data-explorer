import os
import glob
import pandas as pd
import sqlite3
import shutil

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
archive_dir = os.path.join(data_dir, 'geocode_archive')
os.makedirs(archive_dir, exist_ok=True)
db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
table_name = 'masterprovider_with_geocodes_geopy'

# 1. Consolidate all batch CSVs
batch_files = sorted(glob.glob(os.path.join(data_dir, 'masterprovider_with_geocodes_geopy_batch_*.csv')))
if not batch_files:
    print('No batch files found.')
    exit(0)

print(f'Found {len(batch_files)} batch files to consolidate.')
all_batches = pd.concat([pd.read_csv(f, dtype=str) for f in batch_files], ignore_index=True)

# 2. Add to DB (create table if not exists, else append)
conn = sqlite3.connect(db_path)
all_batches.to_sql(table_name, conn, if_exists='append', index=False)
conn.close()
print(f'Appended {len(all_batches)} records to {table_name} in {db_path}')

# 3. Move batch files to archive
for f in batch_files:
    shutil.move(f, os.path.join(archive_dir, os.path.basename(f)))
print(f'Moved batch files to {archive_dir}')

# 4. Query which addresses still need geocoding
master_csv = os.path.join(project_root, 'data/processed/masterprovider_with_penetration.csv')
master = pd.read_csv(master_csv, dtype=str)
if 'PRACTICE LOCATION TYPE' not in master.columns:
    raise ValueError("Column 'PRACTICE LOCATION TYPE' not found in input file.")
master = master[master['PRACTICE LOCATION TYPE'] == 'HHA BRANCH']
cols_needed = ['CCN', 'ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']
master['ZIP CODE'] = master['ZIP CODE'].astype(str).str[:5].str.zfill(5)
unique_master = master[cols_needed].drop_duplicates().reset_index(drop=True)

# Get already geocoded addresses from DB
conn = sqlite3.connect(db_path)
try:
    geocoded = pd.read_sql(f'SELECT {', '.join(cols_needed)} FROM {table_name}', conn)
except Exception:
    geocoded = pd.DataFrame(columns=cols_needed)
conn.close()

remaining = pd.merge(unique_master, geocoded, on=cols_needed, how='left', indicator=True)
remaining = remaining[remaining['_merge'] == 'left_only'][cols_needed]
print(f"{len(remaining)} addresses remain to be geocoded.")

# 5. Export next batch of 100 for geocoding
batch_size = 100
for i in range(0, len(remaining), batch_size):
    batch = remaining.iloc[i:i+batch_size]
    batch_file = os.path.join(data_dir, f'masterprovider_with_geocodes_geopy_batch_{i//batch_size+1}.csv')
    batch.to_csv(batch_file, index=False)
    print(f"Exported batch {i//batch_size+1} with {len(batch)} addresses to {batch_file}")
# After geocoding, rerun this script to consolidate/appends/archive and repeat until done.
