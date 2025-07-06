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
        if col == 'Enrolled':
            # Remove commas and convert to numeric
            df[col] = df[col].str.replace(',', '', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')


# Stratification bins/labels using percentiles for Enrolled
def get_bins_labels(col):
    if col == 'provider_count':
        vals = df['provider_count'].dropna().sort_values()
        if len(vals) < 3:
            return [-0.1, 0.5, 2.5, np.inf], ['none', 'low', 'medium/high']
        q1 = vals.quantile(0.33)
        q2 = vals.quantile(0.66)
        return [-0.1, q1, q2, np.inf], ['low', 'medium', 'high']
    if col == 'closest_provider_distance':
        vals = df['closest_provider_distance'].dropna().sort_values()
        if len(vals) < 3:
            return [-0.1, 25, 60, np.inf], ['close', 'medium', 'far']
        q1 = vals.quantile(0.33)
        q2 = vals.quantile(0.66)
        return [-0.1, q1, q2, np.inf], ['close', 'medium', 'far']
    if col == 'Penetration':
        vals = df['Penetration'].dropna().sort_values()
        if len(vals) < 3:
            return [-0.1, 0.2, 0.4, np.inf], ['low', 'medium', 'high']
        q1 = vals.quantile(0.33)
        q2 = vals.quantile(0.66)
        return [-0.1, q1, q2, np.inf], ['low', 'medium', 'high']
    if col == 'Enrolled':
        # Use 33rd and 66th percentiles for bins
        enrolled_vals = df['Enrolled'].dropna().sort_values()
        if len(enrolled_vals) < 3:
            # Fallback to fixed bins if not enough data
            return [-0.1, 100, 500, np.inf], ['low', 'medium', 'high']
        q1 = enrolled_vals.quantile(0.33)
        q2 = enrolled_vals.quantile(0.66)
        return [-0.1, q1, q2, np.inf], ['low', 'medium', 'high']
    return None, None


# Stratify columns with error handling and print missing columns
for col in ['provider_count', 'closest_provider_distance', 'Penetration', 'Enrolled']:
    if col in df.columns and df[col].notnull().any():
        bins, labels = get_bins_labels(col)
        df[col + '_strata'] = pd.cut(df[col], bins=bins, labels=labels, include_lowest=True)
    else:
        print(f"Column {col} missing or empty, skipping stratification.")


# Define severity and reason columns
required_strata = ['Penetration_strata', 'provider_count_strata', 'closest_provider_distance_strata', 'Enrolled_strata']
if all(col in df.columns for col in required_strata):
    # Code 2: Unmet home health provider needs
    unmet_needs = (
        ((df['Penetration_strata'].isin(['high', 'medium'])) | (df['Enrolled_strata'].isin(['high', 'medium']))) &
        ((df['provider_count_strata'].isin(['none', 'low'])) & (df['closest_provider_distance_strata'] == 'far'))
    )
    # Code 1: Low Medicare population
    low_medicare = (
        ((df['Penetration_strata'] == 'low') | (df['Enrolled_strata'] == 'low')) &
        (df['provider_count_strata'].isin(['none', 'low']))
    )
    # Assign severity, reason, and code
    df['desert_severity'] = np.where(unmet_needs, 'severe',
                              np.where(low_medicare, 'low_medicare', 'not severe'))
    df['severity_reason'] = np.where(unmet_needs, 'Unmet demand for home health provider needs',
                              np.where(low_medicare, 'Low Medicare population', ''))
    df['severity_reason_code'] = np.where(unmet_needs, 2,
                                   np.where(low_medicare, 1, 0))
    # Coverage desert: 1 if any severity reason code is not 0
    df['coverage_desert'] = np.where(df['severity_reason_code'] != 0, 1, 0)
else:
    print("One or more required strata columns missing, cannot compute severity.")
    df['desert_severity'] = 'unknown'
    df['severity_reason'] = ''
    df['severity_reason_code'] = 0
    df['coverage_desert'] = 0

# Summary report
severe_zips = df[df['desert_severity'] == 'severe']


print(f"Total ZIPs with severe home health access issue: {len(severe_zips)}")
if not severe_zips.empty:
    print(severe_zips[['FIPS', 'provider_count', 'closest_provider_distance', 'Penetration', 'Enrolled']].head(20))
    total_enrolled_affected = severe_zips['Enrolled'].sum()
    print(f"Total enrolled population affected by severe desert: {int(total_enrolled_affected)}")

# Also print total enrolled and percent affected by any coverage desert

# Fix: Only count each enrolled population once (avoid double-counting if ZIPs are duplicated)
desert_zips = df[df['coverage_desert'] == 1]
total_enrolled_desert = desert_zips[['FIPS', 'Enrolled']].drop_duplicates(subset='FIPS')['Enrolled'].sum()
print("1-->", total_enrolled_desert)
total_enrolled = df[['FIPS', 'Enrolled']].drop_duplicates(subset='FIPS')['Enrolled'].sum()
print("2-->", total_enrolled)
print("3-->", total_enrolled_desert/ total_enrolled, "  any desert")
print("4-->", total_enrolled_affected/ total_enrolled, "  severe desert")
print("5-->", total_enrolled_affected/ total_enrolled_desert, "  pct of desert")
percent_affected = (total_enrolled_desert / total_enrolled * 100) if total_enrolled > 0 else 0
print(f"Total enrolled population affected by any coverage desert: {int(total_enrolled_desert)}")
print(f"Percent of enrolled population affected by any coverage desert: {percent_affected:.2f}%")

# Save annotated file
df.to_csv(output_csv, index=False)
print(f"Saved severity-stratified coverage desert file to {output_csv}")
