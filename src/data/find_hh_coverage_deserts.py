from tqdm import tqdm
import pandas as pd
import numpy as np
import os
from geopy.distance import geodesic

# Paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
penetration_csv = os.path.join(data_dir, 'State_County_Penetration_MA_2025_06_latlon.csv')
providers_csv = os.path.join(data_dir, 'new_final_master_provider.csv')
output_csv = os.path.join(data_dir, 'hh_coverage_deserts.csv')

# Load data
pen = pd.read_csv(penetration_csv, dtype=str)
prov = pd.read_csv(providers_csv, dtype=str)


# Aggregate provider counts by FIPS
if 'FIPS' in pen.columns and 'FIPS' in prov.columns:
    prov_fips_counts = prov.groupby('FIPS').size().reset_index(name='provider_count')
    pen['FIPS'] = pen['FIPS'].astype(str).str.zfill(5)
    prov_fips_counts['FIPS'] = prov_fips_counts['FIPS'].astype(str).str.zfill(5)
    pen = pen.merge(prov_fips_counts, on='FIPS', how='left')
    pen['provider_count'] = pen['provider_count'].fillna(0).astype(int)
else:
    # fallback to ZIP if FIPS not present
    prov_zip_counts = prov.groupby('ZIP CODE').size().reset_index(name='provider_count')
    pen['ZIP CODE'] = pen['ZIP CODE'].astype(str).str[:5].str.zfill(5)
    prov_zip_counts['ZIP CODE'] = prov_zip_counts['ZIP CODE'].astype(str).str[:5].str.zfill(5)
    pen = pen.merge(prov_zip_counts, on='ZIP CODE', how='left')
    pen['provider_count'] = pen['provider_count'].fillna(0).astype(int)

# Identify deserts (zips with 0 providers)
pen['is_coverage_desert'] = pen['provider_count'] == 0

total_zips = len(pen)
desert_zips = pen[pen['is_coverage_desert']]
num_deserts = len(desert_zips)

# Quality report
print(f"Total ZIPs: {total_zips}")
print(f"ZIPs with no HH provider: {num_deserts}")
if 'ENROLLED' in pen.columns and 'ELIGIBLES' in pen.columns and 'PENETRATION_RATE' in pen.columns:
    total_enrolled = desert_zips['ENROLLED'].astype(float).sum()
    total_eligibles = desert_zips['ELIGIBLES'].astype(float).sum()
    avg_pen_rate = desert_zips['PENETRATION_RATE'].astype(float).mean()
    print(f"Total enrolled in deserts: {total_enrolled}")
    print(f"Total eligibles in deserts: {total_eligibles}")
    print(f"Average penetration rate in deserts: {avg_pen_rate:.2f}")

# For deserts, find closest provider (with progress bar)
results = []
prov_valid = prov[prov['lat'].notnull() & prov['lon'].notnull() & prov['CCN'].notnull() & (prov['CCN'] != '')]

# Use latitude/longitude columns for desert_zips if present
for idx, row in tqdm(desert_zips.iterrows(), total=num_deserts, desc='Finding closest provider for deserts'):
    if 'latitude' in row and 'longitude' in row:
        try:
            zip_lat = float(row['latitude'])
            zip_lon = float(row['longitude'])
        except Exception:
            zip_lat = np.nan
            zip_lon = np.nan
    else:
        zip_lat = row.get('lat', np.nan)
        zip_lon = row.get('lon', np.nan)
    zip_latlon = (zip_lat, zip_lon)
    if np.isnan(zip_latlon[0]) or np.isnan(zip_latlon[1]):
        results.append({'closest_provider_ccn': np.nan, 'closest_provider_distance': np.nan})
        continue
    prov_valid['distance'] = prov_valid.apply(lambda p: geodesic(zip_latlon, (p['lat'], p['lon'])).miles, axis=1)
    closest = prov_valid.loc[prov_valid['distance'].idxmin()] if not prov_valid.empty else None
    results.append({
        'closest_provider_ccn': closest['CCN'] if closest is not None else np.nan,
        'closest_provider_distance': closest['distance'] if closest is not None else np.nan
    })

# Add closest provider info to deserts
if results:
    desert_zips = desert_zips.copy()
    desert_zips[['closest_provider_ccn', 'closest_provider_distance']] = pd.DataFrame(results, index=desert_zips.index)
    # Merge back to pen
    pen = pen.merge(desert_zips[['ZIP CODE', 'closest_provider_ccn', 'closest_provider_distance']], on='ZIP CODE', how='left')

# Save results
pen.to_csv(output_csv, index=False)
print(f"Saved home health coverage desert analysis to {output_csv}")




# # Use correct column names for penetration file
# if 'latitude' in pen.columns and 'longitude' in pen.columns:
#     pen['lat'] = pd.to_numeric(pen['latitude'], errors='coerce')
#     pen['lon'] = pd.to_numeric(pen['longitude'], errors='coerce')
# else:
#     pen['lat'] = pd.to_numeric(pen['lat'], errors='coerce')
#     pen['lon'] = pd.to_numeric(pen['lon'], errors='coerce')
# prov['lat'] = pd.to_numeric(prov['lat'], errors='coerce')
# prov['lon'] = pd.to_numeric(prov['lon'], errors='coerce')

# # Only keep providers with valid lat/lon and CCN
# prov = prov[prov['lat'].notnull() & prov['lon'].notnull() & prov['lat'].notna() & prov['lon'].notna() & (prov['lat'] != '') & (prov['lon'] != '')]
# prov = prov[prov['CCN'].notnull() & (prov['CCN'] != '')]

# # For each ZIP in pen, find providers within 25/60 miles and closest provider
# def find_providers(row):
#     zip_latlon = (row['lat'], row['lon'])
#     if np.isnan(zip_latlon[0]) or np.isnan(zip_latlon[1]):
#         return pd.Series({
#             'has_provider_within_zip': np.nan,
#             'providers_within_25_miles': np.nan,
#             'providers_within_60_miles': np.nan,
#             'closest_provider_ccn': np.nan,
#             'closest_provider_distance': np.nan
#         })
#     prov['distance'] = prov.apply(lambda p: geodesic(zip_latlon, (p['lat'], p['lon'])).miles, axis=1)
#     within_25 = prov[prov['distance'] <= 25]
#     within_60 = prov[prov['distance'] <= 60]
#     closest = prov.loc[prov['distance'].idxmin()] if not prov.empty else None
#     return pd.Series({
#         'has_provider_within_zip': int(len(within_25) > 0),
#         'providers_within_25_miles': len(within_25),
#         'providers_within_60_miles': len(within_60),
#         'closest_provider_ccn': closest['CCN'] if closest is not None else np.nan,
#         'closest_provider_distance': closest['distance'] if closest is not None else np.nan
#     })

# # Apply to all rows
# results = pen.apply(find_providers, axis=1)
# pen = pen.join(results)

# # Filter for coverage deserts (no provider within ZIP)
# pen['is_coverage_desert'] = pen['has_provider_within_zip'] == 0

# # Save results
# pen.to_csv(output_csv, index=False)
# print(f"Saved home health coverage desert analysis to {output_csv}")
