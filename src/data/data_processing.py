import subprocess
import sys
import os

def run_script(script_path):
    print(f"Running: {script_path}")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

if __name__ == "__main__":
    # Always run from src/data so db and script are in the same folder
    os.chdir(os.path.dirname(__file__))
    # Get project root (three levels up from this file)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    # Find the data folder in the tree from root
    data_folder = os.path.join(project_root, 'data')
    print(f"Project root: {project_root}")
    print(f"Data folder: {data_folder}")
    # 1. Build master crosswalk
    run_script('combine_zip_county_cbsa.py')
    # 2. Build provider master (if needed)
    run_script('provider_hhcahps_master.py')
    # 3. Enrich provider with crosswalk
    run_script('enrich_provider_with_zip_county_cbsa.py')
    # 4. Create master provider from enrollment
    run_script('create_masterprovider_from_enrollment.py')
    # 5. Join master with penetration data
    run_script('join_masterprovider_with_penetration.py')

    # 6. (Optional) Add ZIP lat/lon to penetration file
    # Only run add_zip_latlon_to_penetration.py
    run_script('add_zip_latlon_to_penetration.py')

    # 7. Load masterprovider_with_penetration.csv into cms_homehealth.db
    # (Moved to join_masterprovider_with_penetration.py)

    # Ensure DB is gitignored
    # Ensure DB is gitignored (moved to join_masterprovider_with_penetration.py)

    # 7. Automated geocoding and consolidation (Nominatim/geopy)
    #run_script('batch_geocode_provider_addresses_geopy.py')

    # 8. Automated geocoding and consolidation (Nominatim/geopy)
    run_script('report_geocode_coverage.py')

    # 9. Automated geocoding and consolidation (Nominatim/geopy)
    #run_script('geocode_addresses_with_google.py')

    # 10. Merge Google geocoding results with existing geocoded addresses
    run_script('merge_and_report_google_geocoding.py')

    # 11. Merge final lat/lon to master provider
    run_script('merge_final_latlon_to_master.py')

    print("\nAll data processing steps completed.")

   