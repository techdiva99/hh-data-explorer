import pandas as pd
import numpy as np
import os

def stratify(val, bins, labels):
    return pd.cut([val], bins=bins, labels=labels, include_lowest=True)[0]

# Paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
deserts_csv = os.path.join(data_dir, 'hh_coverage_deserts.csv')
output_csv = os.path.join(data_dir, 'hh_coverage_deserts_severity.csv')

# Load data
df = pd.read_csv(deserts_csv, dtype=str)
print("Columns in file:", df.columns.tolist())
# Convert relevant columns to numeric (force for ENROLLED and PENETRATION_RATE)
for col in ['provider_count', 'closest_provider_distance', 'Penetration', 'Enrolled']:
    if col in df.columns:
        # Remove percent sign and convert to float for Penetration if needed
        if col == 'Penetration':
            df[col] = df[col].str.replace('%', '', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Stratification bins/labels
def get_bins_labels(col):
    if col == 'provider_count':
        return [-0.1, 0.5, 2.5, np.inf], ['none', 'low', 'medium/high']
    if col == 'closest_provider_distance':
        return [-0.1, 25, 60, np.inf], ['close', 'medium', 'far']
    if col == 'Penetration':
        return [-0.1, 0.2, 0.4, np.inf], ['low', 'medium', 'high']
    if col == 'Enrolled':
        return [-0.1, 100, 500, np.inf], ['low', 'medium', 'high']
    return None, None


# Stratify columns with error handling and print missing columns
for col in ['provider_count', 'closest_provider_distance', 'Penetration', 'Enrolled']:
    if col in df.columns and df[col].notnull().any():
        bins, labels = get_bins_labels(col)
        df[col + '_strata'] = pd.cut(df[col], bins=bins, labels=labels, include_lowest=True)
    else:
        print(f"Column {col} missing or empty, skipping stratification.")

# Define severity only if all strata columns exist
required_strata = ['PENETRATION_RATE_strata', 'provider_count_strata', 'closest_provider_distance_strata', 'ENROLLED_strata']
if all(col in df.columns for col in required_strata):
    severe = (
        (df['PENETRATION_RATE_strata'] == 'high') &
        (df['provider_count_strata'].isin(['none', 'low'])) &
        (df['closest_provider_distance_strata'] == 'far') &
        (df['ENROLLED_strata'] == 'high')
    )
    df['desert_severity'] = np.where(severe, 'severe', 'not severe')
else:
    print("One or more required strata columns missing, cannot compute severity.")
    df['desert_severity'] = 'unknown'

# Summary report
severe_zips = df[df['desert_severity'] == 'severe']
print(f"Total ZIPs with severe home health access issue: {len(severe_zips)}")
if not severe_zips.empty:
    print(severe_zips[['FIPS', 'provider_count', 'closest_provider_distance', 'Penetration', 'Enrolled']].head(20))

# Save annotated file
df.to_csv(output_csv, index=False)
print(f"Saved severity-stratified coverage desert file to {output_csv}")
