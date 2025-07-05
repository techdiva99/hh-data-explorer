import pandas as pd
import os, json

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Read file names from data/datamap.json
datamap_path = os.path.join(project_root, 'data', 'datamap.json')
with open(datamap_path, 'r') as f:
    datamap = json.load(f)

# Paths
#project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
enrollment_csv = os.path.join(project_root, datamap['hh_enrollment'])
master_provider_csv = os.path.join(project_root, 'data/processed/master_provider_for_review.csv')
crosswalk_csv = os.path.join(project_root, 'data/processed/zip_county_cbsa_cbsaenriched.csv')
output_csv = os.path.join(project_root, 'data/processed/masterprovider_from_enrollment.csv')


# Load data with encoding fallback
def robust_read_csv(path):
    try:
        return pd.read_csv(path, dtype=str)
    except UnicodeDecodeError:
        try:
            return pd.read_csv(path, dtype=str, encoding='latin1')
        except Exception as e:
            raise RuntimeError(f"Could not read {path}: {e}")

enroll = robust_read_csv(enrollment_csv)
master = robust_read_csv(master_provider_csv)

# Standardize CCN columns
if 'CCN' in enroll.columns:
    enroll['CCN'] = enroll['CCN'].astype(str).str.zfill(6)
else:
    raise ValueError('CCN column not found in enrollment file.')
if 'CCN' in master.columns:
    master['CCN'] = master['CCN'].astype(str).str.zfill(6)
else:
    # Try to find the right column
    ccn_cols = [c for c in master.columns if 'CCN' in c]
    if ccn_cols:
        master['CCN'] = master[ccn_cols[0]].astype(str).str.zfill(6)
    else:
        raise ValueError('CCN column not found in master provider file.')


# Merge: all enrollment, all master columns (right join)
merged = pd.merge(enroll, master, on='CCN', how='left', suffixes=('_enroll', ''))

# Calculate Estimated Patient Volume
def to_float(val):
    try:
        return float(str(val).replace(',', '').replace('%',''))
    except:
        return float('nan')

if 'Number of completed Surveys' in merged.columns and 'Survey response rate' in merged.columns:
    num_surveys = merged['Number of completed Surveys'].apply(to_float)
    response_rate = merged['Survey response rate'].apply(to_float)
    est_pat_vol = num_surveys * 100 / response_rate
    merged['Estimated Patient Volume'] = est_pat_vol.round(0).astype('Int64')
else:
    merged['Estimated Patient Volume'] = pd.NA

# If CBSA/county missing, try to fill from crosswalk (optional, not implemented here)
# crosswalk = pd.read_csv(crosswalk_csv, dtype=str)
# ...

# Save result
merged.to_csv(output_csv, index=False)
print(f"Master provider directory created: {output_csv} ({len(merged)} records)")
