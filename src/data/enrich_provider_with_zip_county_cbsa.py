import pandas as pd
import json
import os, json

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Read file names from data/datamap.json
datamap_path = os.path.join(project_root, 'data', 'datamap.json')
with open(datamap_path, 'r') as f:
    datamap = json.load(f)

def enrich_provider_with_zip_county_cbsa(
    provider_csv=os.path.join(project_root, datamap['hh_provider']),
    zip_county_cbsa_csv=os.path.join(project_root, 'data/processed/zip_county_cbsa_cbsaenriched.csv'),
    output_csv=os.path.join(project_root, 'data/processed/provider_cbsa_enriched.csv')
):
    # Load files
    provider = pd.read_csv(provider_csv, dtype=str)
    crosswalk = pd.read_csv(zip_county_cbsa_csv, dtype=str)

    print(provider.columns)

    # Standardize ZIP columns
    provider['ZIP Code'] = provider['ZIP Code'].astype(str).str[:5]
    crosswalk['ZIP'] = crosswalk['ZIP'].astype(str).str[:5]


    # Add CCN column (copy of CMS Certification Number (CCN)) before merge
    if 'CMS Certification Number (CCN)' in provider.columns:
        provider['CCN'] = provider['CMS Certification Number (CCN)'].astype(str).str.zfill(6)
    else:
        raise ValueError('CMS Certification Number (CCN) column not found in provider file.')

    # Merge provider with crosswalk on ZIP
    enriched = provider.merge(crosswalk, left_on='ZIP Code', right_on='ZIP', how='left')

    # Drop duplicates: keep first occurrence for each CCN + ZIP Code
    before = len(enriched)
    enriched = enriched.drop_duplicates(subset=['CCN', 'ZIP Code'], keep='first')
    after = len(enriched)
    print(f"Dropped {before-after} duplicate rows based on CCN and ZIP Code.")

    # Save enriched provider file
    enriched.to_csv(output_csv, index=False)
    print(f"Provider file enriched with ZIP/CBSA/county info written to {output_csv} with {len(enriched)} records.")
    print(enriched[['ZIP Code','FIPS_COUNTY','CBSA','cbsatitle','metropolitanmicropolitanstatis']].head())

    # --- NEW: Add provider_cbsa_enriched.csv to cms_homehealth.db ---
    db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
    import sqlite3
    conn = sqlite3.connect(db_path)
    enriched.to_sql('provider_cbsa_enriched', conn, if_exists='replace', index=False)
    print(f"Loaded {len(enriched)} records into provider_cbsa_enriched table in {db_path}")

    # --- NEW: Store master ZIP/CBSA/county crosswalk in DB ---
    zip_cbsa_crosswalk_csv = os.path.join(project_root, 'data/processed/zip_county_cbsa_cbsaenriched.csv')
    if os.path.exists(zip_cbsa_crosswalk_csv):
        zip_cbsa_crosswalk_df = pd.read_csv(zip_cbsa_crosswalk_csv, dtype=str)
        zip_cbsa_crosswalk_df.to_sql('zip_cbsa_county_crosswalk', conn, if_exists='replace', index=False)
        print(f"Loaded {len(zip_cbsa_crosswalk_df)} records into zip_cbsa_county_crosswalk table in {db_path}")
    else:
        print(f"WARNING: {zip_cbsa_crosswalk_csv} not found, crosswalk table not loaded to DB.")

    # --- NEW: Combine with provider_hhcahps from cms_homehealth.db to make master provider table ---
    cms_db_path = os.path.join(os.path.dirname(__file__), 'cms_homehealth.db')
    cms_conn = sqlite3.connect(cms_db_path)
    try:
        provider_hhcahps_df = pd.read_sql('SELECT * FROM provider_hhcahps', cms_conn)
    except Exception as e:
        cms_conn.close()
        raise RuntimeError(f"Could not load provider_hhcahps table from cms_homehealth.db: {e}")
    cms_conn.close()
    # Merge on CCN
    master_provider_df = enriched.merge(provider_hhcahps_df, on='CCN', how='outer', suffixes=('_cbsa', '_hhcahps'))
    # Save to new table and CSV for review
    master_provider_df.to_sql('master_provider', conn, if_exists='replace', index=False)
    master_csv = os.path.join(os.path.dirname(output_csv), 'master_provider_for_review.csv')
    master_provider_df.to_csv(master_csv, index=False)
    print(f"Master provider table created in DB and CSV for review: {master_csv}")
    conn.close()

if __name__ == "__main__":
    enrich_provider_with_zip_county_cbsa()
