import pandas as pd
import os
import numpy as np
import json

# Paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Read file names from data/datamap.json
datamap_path = os.path.join(project_root, 'data', 'datamap.json')
with open(datamap_path, 'r') as f:
    datamap = json.load(f)

penetration_csv = os.path.join(project_root, datamap['state_county_penetration'])

# Use local SimpleMaps Excel file for ZIP geolocation
zip_latlon_xlsx = os.path.join(project_root, datamap['simplemaps_zip_geo'])


if not os.path.exists(zip_latlon_xlsx):
    raise FileNotFoundError(f"Expected ZIP geolocation file not found: {zip_latlon_xlsx}")

# Output for penetration file
output_csv = os.path.join(project_root, 'data/processed/State_County_Penetration_MA_2025_06_latlon.csv')
# Output for masterprovider_with_penetration
masterprov_csv = os.path.join(project_root, 'data/processed/masterprovider_with_penetration.csv')
masterprov_latlon_csv = os.path.join(project_root, 'data/processed/masterprovider_with_penetration_latlon.csv')

# Load data
def robust_read_csv(path):
    try:
        return pd.read_csv(path, dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding='latin1')

pen = robust_read_csv(penetration_csv)

zip_latlon = pd.read_excel(zip_latlon_xlsx, dtype=str)
# Standardize columns and FIPS
zip_latlon = zip_latlon.rename(columns={
    'zip': 'ZIP',
    'lat': 'latitude',
    'lng': 'longitude',
    'county_fips': 'county_fips'
})
if 'county_fips' not in zip_latlon.columns:
    raise ValueError('county_fips column not found in simplemaps ZIP file.')
zip_latlon['county_fips'] = zip_latlon['county_fips'].astype(str).str.zfill(5)



# --- Penetration file ---
if 'FIPS' not in pen.columns:
    raise ValueError('FIPS column not found in penetration file.')
pen['FIPS'] = pen['FIPS'].astype(str).str.zfill(5)
merged = pd.merge(pen, zip_latlon[['county_fips','latitude','longitude']], left_on='FIPS', right_on='county_fips', how='left')
match_pct = 100 * np.mean(~merged['latitude'].isna())
merged.to_csv(output_csv, index=False)
print(f"Penetration file with county_fips lat/lon written to: {output_csv} ({len(merged)} records)")
print(f"county_fips match percent for penetration file: {match_pct:.1f}%")

# --- Masterprovider file ---
if os.path.exists(masterprov_csv):
    masterprov = robust_read_csv(masterprov_csv)
    if 'FIPS' in masterprov.columns:
        masterprov['FIPS'] = masterprov['FIPS'].astype(str).str.zfill(5)
        merged_mp = pd.merge(masterprov, zip_latlon[['county_fips','latitude','longitude']], left_on='FIPS', right_on='county_fips', how='left')
        match_pct_mp = 100 * np.mean(~merged_mp['latitude'].isna())
        merged_mp.to_csv(masterprov_latlon_csv, index=False)
        print(f"masterprovider_with_penetration with county_fips lat/lon written to: {masterprov_latlon_csv} ({len(merged_mp)} records)")
        print(f"county_fips match percent for masterprovider_with_penetration: {match_pct_mp:.1f}%")
    else:
        print('FIPS column not found in masterprovider_with_penetration.csv')
else:
    print(f"masterprovider_with_penetration.csv not found at {masterprov_csv}")
