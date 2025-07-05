import pandas as pd
import os, json
import sqlite3

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Read file names from data/datamap.json
datamap_path = os.path.join(project_root, 'data', 'datamap.json')
with open(datamap_path, 'r') as f:
    datamap = json.load(f)

masterprov_csv = os.path.join(project_root, 'data/processed/masterprovider_from_enrollment.csv')
penetration_csv = os.path.join(project_root, datamap['state_county_penetration'])
output_csv = os.path.join(project_root, 'data/processed/masterprovider_with_penetration.csv')

# Load data with encoding fallback
def robust_read_csv(path):
    try:
        return pd.read_csv(path, dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding='latin1')

master = robust_read_csv(masterprov_csv)
pen = robust_read_csv(penetration_csv)

# Standardize FIPS columns
if 'FIPS_COUNTY' not in master.columns:
    raise ValueError('FIPS_COUNTY column not found in masterprovider_from_enrollment.csv')
if 'FIPS' not in pen.columns:
    raise ValueError('FIPS column not found in State_County_Penetration_MA_2025_06.csv')

# Merge on FIPS_COUNTY (master) and FIPS (penetration)
merged = pd.merge(master, pen[['FIPS','Eligibles','Enrolled','Penetration']], left_on='FIPS_COUNTY', right_on='FIPS', how='left')

# Save result
merged.to_csv(output_csv, index=False)
print(f"Joined masterprovider with penetration data: {output_csv} ({len(merged)} records)")

# --- Load to SQLite DB and ensure .gitignore ---
db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
if os.path.exists(output_csv):
    df = pd.read_csv(output_csv, dtype=str)
    conn = sqlite3.connect(db_path)
    df.to_sql('masterprovider_with_penetration', conn, if_exists='replace', index=False)
    print(f"Loaded {len(df)} records into masterprovider_with_penetration table in {db_path}")
    conn.close()
else:
    print(f"WARNING: {output_csv} not found, not loaded to DB.")

# Ensure DB is gitignored
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
gitignore_path = os.path.join(project_root, '.gitignore')
db_relative = os.path.relpath(db_path, project_root)
with open(gitignore_path, 'a+') as f:
    f.seek(0)
    lines = f.read().splitlines()
    if db_relative not in lines:
        f.write(f"\n{db_relative}\n")
        print(f"Added {db_relative} to .gitignore")
    else:
        print(f"{db_relative} already in .gitignore")
