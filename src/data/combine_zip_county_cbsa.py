import pandas as pd
import json, os

# Set paths relative to project root
#project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print()
datamap_path = os.path.join(project_root, 'data', 'datamap.json')
with open(datamap_path, 'r') as f:
        datamap = json.load(f)

def combine_zip_county_cbsa(
    zip_county_xlsx=None,
    zip_cbsa_xlsx=None #,
    #output_csv=None
):

    
    #project_root = project_root + "/project_root"
    if zip_county_xlsx is None:
        zip_county_xlsx = os.path.join(project_root, datamap['zip_county'])
    if zip_cbsa_xlsx is None:
        zip_cbsa_xlsx = os.path.join(project_root, datamap['zip_cbsa'])

    
    if not os.path.exists(zip_county_xlsx):
        raise FileNotFoundError(f"ZIP_COUNTY file not found: {zip_county_xlsx}")
    if not os.path.exists(zip_cbsa_xlsx):
        raise FileNotFoundError(f"ZIP_CBSA file not found: {zip_cbsa_xlsx}")
    zip_county = pd.read_excel(zip_county_xlsx, dtype=str)
    zip_cbsa = pd.read_excel(zip_cbsa_xlsx, dtype=str)
    zip_county['ZIP'] = zip_county['ZIP'].astype(str).str[:5]
    zip_cbsa['ZIP'] = zip_cbsa['ZIP'].astype(str).str[:5]
    # Rename COUNTY to FIPS_COUNTY
    if 'COUNTY' in zip_county.columns:
        zip_county = zip_county.rename(columns={'COUNTY': 'FIPS_COUNTY'})
    combined = pd.merge(zip_county, zip_cbsa, on='ZIP', how='outer', suffixes=('_county', '_cbsa'))
    # Drop exact duplicate rows
    before = len(combined)
    combined = combined.drop_duplicates()
    after = len(combined)
    # Do not export intermediate CSV, just return DataFrame
    print(f"Combined ZIP_COUNTY and ZIP_CBSA: {after} records (removed {before-after} duplicates).")
    print(combined.head())
    return combined

def add_cbsa_info_to_zip_county_cbsa(
    zip_county_cbsa_csv=None,
    cbsa_fips_csv=None,
    output_csv=None
):

    if cbsa_fips_csv is None:
        cbsa_fips_csv = os.path.join(project_root, datamap['cbsa_fips'])
    if output_csv is None:
        output_csv = os.path.join(project_root, 'data', 'processed', 'zip_county_cbsa_cbsaenriched.csv')
    # Ensure processed directory exists
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    # Accept DataFrame directly
    if isinstance(zip_county_cbsa_csv, pd.DataFrame):
        zip_county_cbsa = zip_county_cbsa_csv
    else:
        if not os.path.exists(zip_county_cbsa_csv):
            raise FileNotFoundError(f"Combined crosswalk file not found: {zip_county_cbsa_csv}")
        zip_county_cbsa = pd.read_csv(zip_county_cbsa_csv, dtype=str)
    if not os.path.exists(cbsa_fips_csv):
        raise FileNotFoundError(f"CBSA FIPS file not found: {cbsa_fips_csv}")
    cbsa_fips = pd.read_csv(cbsa_fips_csv, dtype=str)
    cbsa_fips['FIPS_COUNTY'] = cbsa_fips['fipsstatecode'].str.zfill(2) + cbsa_fips['fipscountycode'].str.zfill(3)
    merged = zip_county_cbsa.merge(
        cbsa_fips[['FIPS_COUNTY','cbsatitle','metropolitanmicropolitanstatis','centraloutlyingcounty','countycountyequivalent']],
        on='FIPS_COUNTY', how='left')
    merged.to_csv(output_csv, index=False)
    print(f"Final master crosswalk written to {output_csv} ({len(merged)} records)")
    print(merged[['ZIP','FIPS_COUNTY','cbsatitle','metropolitanmicropolitanstatis']].head())
    return output_csv


if __name__ == "__main__":
    combined_df = combine_zip_county_cbsa()
    add_cbsa_info_to_zip_county_cbsa(zip_county_cbsa_csv=combined_df)
